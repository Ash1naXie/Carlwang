#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_SCRIPT="$SCRIPT_DIR/start_service_global_media_watch.sh"
PID_FILE="$SCRIPT_DIR/twitter-crawler-global-media-watch.pid"
MONITOR_LOG="$SCRIPT_DIR/monitor_global_media_watch.log"

log_monitor() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$MONITOR_LOG"
}

if [[ -f "$PID_FILE" ]] && ps -p "$(cat "$PID_FILE")" >/dev/null 2>&1; then
  log_monitor "global_media_watch running pid=$(cat "$PID_FILE")"
  exit 0
fi

log_monitor "global_media_watch not running, restarting"
"$SERVICE_SCRIPT" restart >> "$MONITOR_LOG" 2>&1
