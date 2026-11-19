#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_SCRIPT="$SCRIPT_DIR/start_service_retail_core_lists.sh"
PID_FILE="$SCRIPT_DIR/twitter-crawler-retail-core-lists.pid"
MONITOR_LOG="$SCRIPT_DIR/monitor_retail_core_lists.log"

log_monitor() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$MONITOR_LOG"
}

if [[ -f "$PID_FILE" ]] && ps -p "$(cat "$PID_FILE")" >/dev/null 2>&1; then
  log_monitor "retail_core_lists running pid=$(cat "$PID_FILE")"
  exit 0
fi

log_monitor "retail_core_lists not running, restarting"
"$SERVICE_SCRIPT" restart >> "$MONITOR_LOG" 2>&1
