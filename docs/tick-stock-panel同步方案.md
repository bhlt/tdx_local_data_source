# 通达信 → tick-stock-panel 数据同步方案

> 目标：用本机通达信金融终端的 TQ 接口，为 SC_Linux 上的 tick-stock-panel 供数，**不消耗 tdxaidata 积分**。
> 实测时间：2026-11-20
> 面板项目：`SC_Linux:/home/shencan/workspace/github/tick-stock-panel`

---

## 0. 结论

**可行，且不需要改面板后端代码。** 采用「Windows 采集 + Linux 网关 + 面板 YAML 数据源」三段式：

- Windows：`52_push_panel.py` 从通达信取数，主动 push；
- SC_Linux：`53_panel_gateway.py` 落库并按面板的 `data_providers` 契约输出；
- 面板：新增一个 `data/data_sources/tdx_local.yaml` 即可，provider 校验已实机通过。

| 数据集 | 状态 | 数据来源 |
|---|---|---|
| realtime（全市场快照） | ✅ 已实现 | `get_market_snapshot` 种子 + `get_pricevol` 增量 |
| depth5（五档） | ✅ 已实现 | `get_market_snapshot` 的 Buyp/Buyv/Sellp/Sellv |
| minute（1m） | ✅ 已实现 | 历史取本地 1m；当日由快照合成（本地 1m 只到上一交易日） |
| daily | ✅ 已实现 | `get_market_data(period='1d')` |

---

## 1. 架构

```
Windows（通达信已登录）                    SC_Linux
┌────────────────────────┐   HTTP push    ┌──────────────────────────┐
│ 52_push_panel.py       │ ─────────────▶ │ 53_panel_gateway.py      │
│  · get_pricevol 6s     │  gzip+batch    │  · SQLite 幂等落库        │
│  · snapshot 种子       │                │  · /panel/realtime       │
│  · depth 10s           │                │  · /panel/depth5         │
│  · 1m 合成             │                │  · /panel/minute         │
└───────────┬────────────┘                │  · /panel/daily          │
            │ ctypes                      └────────────┬─────────────┘
      ┌─────▼──────┐                                     │ HTTP
      │ 通达信终端 │                                     ▼
      └────────────┘                          ┌─────────────────────┐
                                              │ tick-stock-panel    │
                                              │ data_sources/*.yaml │
                                              └─────────────────────┘
```

为什么不是面板直连 Windows：

- 面板的 `get_realtime()` 是**单请求全量**，且要求覆盖全市场（否则当日 live 分区会被截断）；
- 通达信客户端只在 Windows，且 17709 只监听本机；
- 用本机网关做缓冲，可以吸收采集抖动、断线重连和增量合并。

---

## 2. 字段契约（已对齐）

### realtime（全市场）

```json
{"symbol":"000001.SZ","name":"平安银行","asset_type":"stock",
 "last_price":12.03,"prev_close":12.04,
 "open":11.99,"high":12.08,"low":11.91,"volume":859477,"amount":1034000000,
 "amount_source":"realtime_delta",
 "change_pct":-0.00083,"amplitude":0.0141,"turnover_rate":0.0044,"timestamp":1795142904517}
```

- 必填：`symbol, last_price, prev_close, open, high, low, volume`（面板 `_REQUIRED` 校验）
- **`timestamp` 必须在 YAML 的 `field_map` 中显式映射**，否则面板落盘时 `quote_ts` 为空，自选列表会显示「时间未确认」
- 单位：`volume=手`、`amount=元`、`change_pct/amplitude/turnover_rate` 为**小数**
- `asset_type`：`stock / etf / index / board / bond / reit / other`；面板可直接用
  `GET /panel/realtime?asset_type=stock` 只取股票
- `amount` 只对 `stock / etf` 当日累计；`index / board` 的 `amount` 由成分股成交额汇总
  （`amount_source=constituents`），没有成分映射时为 `null`，**绝不用 volume×price 回填**；
  全量快照默认 `index/board amount=null`（避免直接求和重复计入），显式
  `?asset_type=index,board` 或 `?symbols=指数代码` 才返回汇总值
- 指数/板块的 `volume` 入站时由「股」折算为「手」，与 `daily` 一致
- 同一指数的别名只保留规范代码：`999999.SH` 入库时归一为 `000001.SH`
- 新增 `GET /panel/instruments?asset_type=stock,etf`：返回 symbol / name / asset_type /
  exchange / float_shares，便于面板对齐 ETF 维表
- 新增 `GET /panel/audit`：检查股票成交额比值（0.8~1.2）、指数成交额量级、别名唯一性
- 全市场约 **5584 行（stock）**，面板建议单响应 ≤6000 行

### depth5

```json
{"000009.SZ":{"asks":[[6.20,78],[0,0],[0,0],[0,0],[0,0]],
              "bids":[[6.19,226],[0,0],[0,0],[0,0],[0,0]],
              "ask_volumes":[78,0,0,0,0],"bid_volumes":[226,0,0,0,0],
              "timestamp":1795142904558}}
```

### minute / daily

`minute`：`symbol, datetime, open/high/low/close, volume, amount, freq="1m"`
`daily`：`symbol, date, open/high/low/close, volume, amount`

---

## 3. 实测结果

| 项目 | 结果 |
|---|---|
| 全市场 `get_pricevol` | 5584 只 **4 秒**（稳态） |
| 全市场 seed 快照 | 5584 只约 **20~25 秒**（8 线程） |
| 单只快照 | 8.9ms |
| 500 只五档 | 约 3.3 秒 |
| 500 只 1m（count=2） | 1.55 秒 |
| 全市场 1m（count=2） | 17.2 秒（因此默认只同步中证500） |
| 本地 1m 历史 | 只到**上一交易日 15:00**（盘中不更新） |
| 契约自检 | **PASS**（300 行 realtime 无缺失；depth 5 档；minute 240 行；daily 20 行） |
| YAML 校验 | 面板 `validate()` 四类数据集全部 **PASS** |

自检输出：

```
[realtime] rows=300 missing=0
sample last=12.03 prev=12.04 open=11.99 high=12.08 low=11.91 vol=859477
[depth5] 000009.SZ levels=5 vols=5 ok=True
[minute] rows=240 last={'datetime':'2026-11-19 15:00:00',...}
[daily]  rows=20  last={'date':'2026-11-20','close':6.21,...}
CONTRACT: PASS
```

---

## 4. 部署

### 4.1 SC_Linux：启动网关

```bash
# 把 53_panel_gateway.py 放到 /opt/tick-panel/
export PANEL_TOKEN='<强密码>'
python3 /opt/tick-panel/53_panel_gateway.py \
  --host 0.0.0.0 --port 18082 \
  --db /var/lib/tick-panel/panel.db
```

systemd：

```ini
[Unit]
Description=TDX panel gateway
After=network.target

[Service]
Environment=PANEL_TOKEN=<强密码>
ExecStart=/usr/bin/python3 /opt/tick-panel/53_panel_gateway.py --host 0.0.0.0 --port 18082 --db /var/lib/tick-panel/panel.db
Restart=always

[Install]
WantedBy=multi-user.target
```

> 绑定 `0.0.0.0` 供 Windows 推送；务必用防火墙只放行 Windows 主机 IP，并保留 token。

### 4.2 Windows：启动采集代理

```powershell
set TDX_PYPLUGINS=d:\program\new_tdx64\PYPlugins\user
set PANEL_TOKEN=<强密码>
python agent\52_push_panel.py `
  --receiver http://<linux-ip>:18082 --market 5 --interval 6 `
  --depth-market 24 --depth-limit 500 `
  --minute-market 24 --minute-limit 500 --daily --daily-count 250
```

- 首次启动会做：名称/流通股本 → 全市场 seed → 1m 历史 240 根 → 日线 250 根 → 进入 6 秒循环
- 建议用计划任务/服务常驻；客户端断线时代理会报错，恢复后自动继续

### 4.3 面板：接入数据源

把 `deploy/tick-stock-panel/data_sources_tdx_local.yaml` 复制到面板的 `data/data_sources/`，然后：

```bash
curl -X POST http://127.0.0.1:3018/api/settings/data-sources/reload
```

在面板设置里把 `realtime / depth5 / minute / daily` 的 provider 切到 `tdx_local`，或在 `data/user_data/preferences.json` 中设置。

> `PANEL_TOKEN` 必须在面板进程环境或项目根 `.env` 中（`token_env` 读取环境变量/.env，不读 `secrets.json`）。不想鉴权就删掉 YAML 的 `auth` 段，同时网关也用默认 token。

---

## 5. 数据口径与限制

| # | 事项 | 说明 / 处置 |
|---|---|---|
| 1 | realtime 必须全市场 | 面板会用全市场行覆写当日 live 分区；只推自选会被截断。代理按全部 A 股推送，并对停牌/无快照股票用 `get_pricevol` 兜底，保证行数完整 |
| 2 | 当日 1m 线 | 本地 1m 只到上一交易日；代理用 6 秒快照合成当日分钟线（标签与通达信一致：09:30~09:31 记为 09:31），分钟翻转时落库 |
| 3 | 五档只有一档 | 客户端不在报价页面时，买二~买五/卖二~卖五可能为空；这是客户端限制，不是链路问题。打开该股报价页可获取完整五档 |
| 4 | 复权 | 日线/分钟线用 `none`（原始价），避免 `back` 随查询窗口漂移；复权由面板的 `adj_factor` 处理 |
| 5 | 单位 | `volume` 统一手、`amount` 统一元；代理已从「股/万元」换算 |
| 6 | 全市场 1m 太慢 | 17 秒/次，默认只同步中证500；需要更大池子时调大 `--minute-limit` 并降低频率 |
| 7 | 断线 | 网关幂等（`batch_id` 去重）；代理失败只丢当次增量，下一轮补齐 |
| 8 | 带宽 | 增量推送 gzip 后约 10~50KB/批；面板拉取全市场 realtime 约 2.5MB JSON/次（本机回环，可接受） |

---

## 6. 运维

```bash
# 网关健康
curl -H "X-Api-Key: $PANEL_TOKEN" http://127.0.0.1:18082/panel/health
# {"ok":true,"panel_state":5584,"depth":500,"bars":...,"last_ts_ms":...}

# 契约自检
python tools/54_check_panel_contract.py --base http://127.0.0.1:18082 --token $PANEL_TOKEN
```

面板侧排障：

- `/api/settings/capability-matrix` 看 `tdx_local` 是否被识别；
- `POST /api/settings/data-sources/test` 用 `{provider, dataset, symbols}` 试拉；
- 若 realtime 为空：先查网关 `/panel/health` 的 `panel_state` 是否为 0。

---

## 7. 可选增强

1. **插件方式**：如果希望面板直连 SSE（而不是 6 秒轮询），可在 `backend/app/plugins/tdx_local/` 写 provider，后台线程消费 `/panel/stream`；当前 YAML 方案已够用。
2. **tick 明细**：本地客户端不提供逐笔，面板若必须做逐笔成交，仍需 tdxaidata 或 L2；其余数据可以全部走本方案，节省积分。
3. **多市场**：`--market 51/52`（创业板/科创板）、`--market 23`（沪深300）可直接切换股票池。

---

## 8. 产物

```
agent/52_push_panel.py              Windows 采集代理
gateway/53_panel_gateway.py           Linux 数据网关
tools/54_check_panel_contract.py    契约自检
deploy/tick-stock-panel/data_sources_tdx_local.yaml  面板数据源配置
reports/panel_gw.log                  网关日志
data/relay/panel.db                   网关落库
```

