#!/usr/bin/env bash
# 安装 Linux 数据网关（systemd user 服务）
#
# 用法：
#   ./deploy/systemd/install.sh
#   PANEL_TOKEN=xxx PORT=18082 ./deploy/systemd/install.sh
#
# 环境变量：
#   TICK_PANEL_HOME  安装目录，默认 ~/tick-panel
#   PANEL_TOKEN      网关 token；不传则随机生成
#   HOST / PORT      监听地址，默认 0.0.0.0:18082
#   PYTHON           解释器，默认 python3
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
INSTALL_DIR="${TICK_PANEL_HOME:-$HOME/tick-panel}"
HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-18082}"
PYTHON="${PYTHON:-python3}"
PANEL_API="${PANEL_API:-http://127.0.0.1:3018}"
TOKEN="${PANEL_TOKEN:-}"

if [[ -z "$TOKEN" ]]; then
  TOKEN="$("$PYTHON" -c 'import secrets; print(secrets.token_hex(16))')"
fi

echo "==> 安装目录: $INSTALL_DIR"
mkdir -p "$INSTALL_DIR/bin" "$INSTALL_DIR/data" "$INSTALL_DIR/logs"
install -m 0644 "$REPO_ROOT/gateway/53_panel_gateway.py" "$INSTALL_DIR/bin/53_panel_gateway.py"
install -m 0755 "$REPO_ROOT/tools/63_panel_instruments_sync.py" "$INSTALL_DIR/bin/63_panel_instruments_sync.py"
install -m 0755 "$REPO_ROOT/deploy/tick-stock-panel/run_instruments_sync.sh" "$INSTALL_DIR/bin/run_instruments_sync.sh"

umask 077
{
  echo "PANEL_TOKEN=$TOKEN"
} > "$INSTALL_DIR/gateway.env"

UNIT_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
mkdir -p "$UNIT_DIR"
cat > "$UNIT_DIR/tdx-panel-gateway.service" <<EOF
[Unit]
Description=TDX panel gateway
After=network.target

[Service]
EnvironmentFile=$INSTALL_DIR/gateway.env
ExecStart=$PYTHON $INSTALL_DIR/bin/53_panel_gateway.py --host $HOST --port $PORT --db $INSTALL_DIR/data/panel.db
Restart=always
RestartSec=3

[Install]
WantedBy=default.target
EOF

echo "==> 启动 systemd user 服务"
systemctl --user daemon-reload
systemctl --user enable --now tdx-panel-gateway
loginctl enable-linger "$USER" >/dev/null 2>&1 || true

# 收盘后刷新一次行情：日线管道会重算 enriched，可能清空 quote_ts，
# 刷新可让面板把 quote_ts 写回，避免自选显示「时间未确认」。
CURL_BIN="$(command -v curl || true)"
if [[ -n "$CURL_BIN" ]]; then
  cat > "$UNIT_DIR/tdx-panel-refresh.service" <<EOF
[Unit]
Description=Refresh tick-stock-panel quotes after daily pipeline

[Service]
Type=oneshot
ExecStart=$CURL_BIN -fsS -X POST $PANEL_API/api/intraday/refresh
EOF
  cat > "$UNIT_DIR/tdx-panel-refresh.timer" <<EOF
[Unit]
Description=Refresh panel quotes after close

[Timer]
OnCalendar=Mon..Fri 15:40
OnCalendar=Mon..Fri 16:10
Unit=tdx-panel-refresh.service

[Install]
WantedBy=timers.target
EOF
  systemctl --user daemon-reload
  systemctl --user enable --now tdx-panel-refresh.timer
else
  echo "WARN: 未找到 curl，跳过收盘刷新定时器（可手动 POST $PANEL_API/api/intraday/refresh）" >&2
fi

# 面板每天 09:10 / 15:35 同步标的维表；单个交易所超时会整块缺失，
# 表现为「名称待同步」、换手率/市值为空。交易日 09:25 / 15:50 补一次。
cat > "$UNIT_DIR/tdx-panel-instruments.service" <<EOF
[Unit]
Description=Backfill tick-stock-panel instruments (names and shares)

[Service]
Type=oneshot
EnvironmentFile=$INSTALL_DIR/gateway.env
ExecStart=$INSTALL_DIR/bin/run_instruments_sync.sh
EOF
cat > "$UNIT_DIR/tdx-panel-instruments.timer" <<EOF
[Unit]
Description=Backfill tick-stock-panel instruments after provider syncs

[Timer]
OnCalendar=Mon..Fri 09:25
OnCalendar=Mon..Fri 15:50
Persistent=true
Unit=tdx-panel-instruments.service

[Install]
WantedBy=timers.target
EOF
systemctl --user daemon-reload
systemctl --user enable --now tdx-panel-instruments.timer

sleep 2
if command -v curl >/dev/null 2>&1; then
  if curl -fsS -H "X-Api-Key: $TOKEN" "http://127.0.0.1:$PORT/panel/health" >/dev/null; then
    echo "==> 健康检查通过: http://127.0.0.1:$PORT/panel/health"
  else
    echo "WARN: 健康检查失败，查看日志: journalctl --user -u tdx-panel-gateway -n 50" >&2
  fi
fi

cat <<EOF

安装完成。

  安装目录   : $INSTALL_DIR
  服务状态   : systemctl --user status tdx-panel-gateway
  日志       : journalctl --user -u tdx-panel-gateway -f
  PANEL_TOKEN: $TOKEN

Windows 采集代理：
  python agent/52_push_panel.py --receiver http://<本机IP>:$PORT --market 5 --include-markets 9,31
  或使用 deploy/windows/install-startup.ps1 配置开机自启。

面板数据源 URL（Docker 内需用 Docker 网关 IP）：
  http://172.25.0.1:$PORT  或  http://<本机IP>:$PORT
EOF
