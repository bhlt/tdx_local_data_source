# 部署脚本

## Linux（数据网关）

```bash
# 自动生成 systemd user 服务、token、数据目录，并启动
./deploy/systemd/install.sh

# 自定义
PANEL_TOKEN=your-secret PORT=18082 TICK_PANEL_HOME=~/tick-panel \
  ./deploy/systemd/install.sh

# 卸载（保留数据）
./deploy/systemd/uninstall.sh
# 彻底删除
./deploy/systemd/uninstall.sh --purge
```

脚本做的事：

1. 创建 `~/tick-panel/{bin,data,logs}`
2. 复制 `gateway/53_panel_gateway.py`、`tools/63_panel_instruments_sync.py`
3. 写入 `~/tick-panel/gateway.env`（600 权限）
4. 生成 `~/.config/systemd/user/tdx-panel-gateway.service`
5. `systemctl --user enable --now`，并执行 `loginctl enable-linger`
6. 调用 `/panel/health` 自检
7. 安装 `tdx-panel-refresh.timer`：工作日 15:40 / 16:10 调用 `/api/intraday/refresh`
8. 安装 `tdx-panel-instruments.timer`：工作日 09:25 / 15:50 补齐面板标的维表

> 为什么需要收盘刷新：面板的日线管道重算 enriched 时可能清空 `quote_ts`，导致自选列表显示「时间未确认」。刷新会让实时链路把 `quote_ts` 写回。
>
> 为什么需要维表兜底：面板拉 `instruments` 时单个交易所超时会整块缺失，表现为「名称待同步」和换手率/市值为空。

模板见 `systemd/tdx-panel-gateway.service`、`systemd/tdx-panel-refresh.timer`、`tick-stock-panel/tdx-panel-instruments.timer`、`env/gateway.env.example`。

## Windows（采集代理）

```powershell
powershell -ExecutionPolicy Bypass -File deploy\windows\install-startup.ps1 `
  -Token <与网关注册一致的 token> `
  -Receiver http://<linux-ip>:18082 `
  -TdxPyPlugins C:\new_tdx64\PYPlugins\user
```

脚本做的事：

1. 生成 `agent/52_push_panel.cmd`（含 token、循环重启、日志重定向）
2. 在用户启动目录放置 `tdx-panel-push.cmd`
3. 立即启动一次（`-NoStart` 可跳过）

### 全市场分钟落库

代理默认带 `--minute-archive-at 08:30,15:45`，每天异步把全市场 1m/5m 落成本地 Parquet：

```
data/minute/1m/date=2026-11-19/part.parquet
data/minute/5m/date=2026-11-19/part.parquet
```

- 列：`symbol, datetime, open, high, low, close, volume, amount`
- 单位已换算成**手 / 元**，与日线一致
- 全市场单日约 134 万行（1m，约 14 MB）、27 万行（5m，约 3.4 MB）
- 全市场 1m 取数约 15 分钟，放在子进程跑，不阻塞实时推送
- 客户端分钟线约保留 3 个月，当天数据要等次日凌晨下载，所以按“缺失分区”回补最近 3 天

手动回补全部本地历史：

```powershell
python tools/64_archive_minute.py --days 90 --out data/minute
# 只补 5 分钟线
python tools/64_archive_minute.py --periods 5m --days 90
```

依赖 `pyarrow`（Parquet）。没有时脚本自动退回 `.csv.gz`（体积约 5 倍）。

> 落库只做本地研究，不推给面板；面板的 minute 数据集仍是代理实时合成的 500 只。

> 使用用户启动目录而不是计划任务，是因为创建 ONLOGON 计划任务通常需要管理员权限。

## 面板（tick-stock-panel）

```bash
cp deploy/tick-stock-panel/data_sources_tdx_local.yaml \
   <panel>/data/data_sources/tdx_local.yaml
curl -X POST http://127.0.0.1:3018/api/settings/data-sources/reload
```

Docker 部署时，YAML 中的 URL 必须使用 **Docker 网关 IP**（如 `172.25.0.1:18082`），不能用 `127.0.0.1`。

### 标的维表兜底（名称 / 股本）

面板每天 09:10 / 15:35 通过 tickflow 拉 `instruments` 维表。单个交易所（尤其沪市 2000+ 只）超时会整块缺失，导致：

- 涨停梯队、自选显示「名称待同步」
- 该市场换手率、流通市值为空

安装脚本会启用 `tdx-panel-instruments.timer`，交易日 09:25 / 15:50 各补一次：

```bash
systemctl --user list-timers tdx-panel-instruments.timer
systemctl --user start tdx-panel-instruments.service   # 手动触发
journalctl --user -u tdx-panel-instruments.service -n 30
```

脚本做的事：

1. 从网关 `/panel/realtime` 取全市场 `symbol + name`
2. 从 `<panel>/data/instruments_extra/shares.csv` 取股本（独立账本，面板覆盖不影响）
3. 补回 `instruments.parquet` 缺失的股票，并 `POST /api/data/refresh-cache`

手工补齐（首次部署或新股上市后更新股本）：

```powershell
# Windows：从通达信本地导出全市场维表
$env:PANEL_TOKEN = "<token>"
python tools/61_export_instruments.py `
  --gateway http://<linux-ip>:18082 --markets all --out reports/instruments.csv
```

```bash
# Linux：合并到面板（容器内执行）
docker cp reports/instruments.csv <container>:/tmp/instruments.csv
docker cp tools/62_apply_instruments.py <container>:/tmp/
docker exec <container> /app/.venv/bin/python /tmp/62_apply_instruments.py \
  --data-dir /app/data --csv /tmp/instruments.csv
curl -X POST http://127.0.0.1:3018/api/data/refresh-cache
```

> 日常无需手工操作；只有新股上市或股本变动较多时，才需要重新导出一次账本。
