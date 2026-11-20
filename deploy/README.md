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
2. 复制 `gateway/53_panel_gateway.py`
3. 写入 `~/tick-panel/gateway.env`（600 权限）
4. 生成 `~/.config/systemd/user/tdx-panel-gateway.service`
5. `systemctl --user enable --now`，并执行 `loginctl enable-linger`
6. 调用 `/panel/health` 自检
7. 安装 `tdx-panel-refresh.timer`：工作日 15:40 / 16:10 调用 `/api/intraday/refresh`

> 为什么需要收盘刷新：面板的日线管道重算 enriched 时可能清空 `quote_ts`，导致自选列表显示「时间未确认」。刷新会让实时链路把 `quote_ts` 写回。

模板见 `systemd/tdx-panel-gateway.service`、`systemd/tdx-panel-refresh.timer`、`env/gateway.env.example`。

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

> 使用用户启动目录而不是计划任务，是因为创建 ONLOGON 计划任务通常需要管理员权限。

## 面板（tick-stock-panel）

```bash
cp deploy/tick-stock-panel/data_sources_tdx_local.yaml \
   <panel>/data/data_sources/tdx_local.yaml
curl -X POST http://127.0.0.1:3018/api/settings/data-sources/reload
```

Docker 部署时，YAML 中的 URL 必须使用 **Docker 网关 IP**（如 `172.25.0.1:18082`），不能用 `127.0.0.1`。
