#!/usr/bin/env bash
# 卸载 Linux 数据网关
#   ./deploy/systemd/uninstall.sh            # 停止并移除服务，保留数据
#   ./deploy/systemd/uninstall.sh --purge    # 同时删除 ~/tick-panel
set -euo pipefail

systemctl --user disable --now tdx-panel-gateway 2>/dev/null || true
rm -f "${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user/tdx-panel-gateway.service"
systemctl --user daemon-reload

if [[ "${1:-}" == "--purge" ]]; then
  rm -rf "${TICK_PANEL_HOME:-$HOME/tick-panel}"
  echo "已删除 ${TICK_PANEL_HOME:-$HOME/tick-panel}"
else
  echo "服务已移除，数据保留在 ${TICK_PANEL_HOME:-$HOME/tick-panel}"
fi
