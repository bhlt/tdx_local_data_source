#!/usr/bin/env bash
# 补齐 tick-stock-panel 维表（名称 + 股本）。
#
# 由 tdx-panel-instruments.timer 在工作日 09:25 / 15:50 触发：
# 面板 09:10、15:35 会同步维表，若某交易所超时会整块缺失，这里补回来。
#
# 依赖：
#   - 面板容器名包含 TickFlow_Stock_Panel（可用 PANEL_CONTAINER 覆盖）
#   - gateway.env 中的 PANEL_TOKEN
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="$HERE/63_panel_instruments_sync.py"
CONTAINER="${PANEL_CONTAINER:-$(docker ps -qf name=TickFlow_Stock_Panel | head -1)}"

if [[ -z "$CONTAINER" ]]; then
  echo "面板容器未运行（name~TickFlow_Stock_Panel），跳过"
  exit 0
fi
if [[ ! -f "$SCRIPT" ]]; then
  echo "找不到 $SCRIPT" >&2
  exit 2
fi
if [[ -z "${PANEL_TOKEN:-}" ]]; then
  echo "缺少 PANEL_TOKEN（应由 systemd EnvironmentFile 提供）" >&2
  exit 2
fi

exec docker exec -i -e PANEL_TOKEN="$PANEL_TOKEN" "$CONTAINER" \
  /app/.venv/bin/python - < "$SCRIPT"
