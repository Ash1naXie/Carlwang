#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON_BIN="${CARLWANG_PYTHON_BIN:-$PROJECT_ROOT/venv/bin/python}"

: "${PROCESS_MONITOR_BASE_URL:=https://luckycoin.ai}"
: "${PROCESS_MONITOR_HOST:=db002}"

SERVICE_NAME="twitter-crawler-global-media-watch"
PID_FILE="$SCRIPT_DIR/${SERVICE_NAME}.pid"
LOG_FILE="$SCRIPT_DIR/service_global_media_watch.log"
PROCESS_CODE="db002_global_media_watch"
LIST_ID="2051959526426038539"

DEFAULT_INTERVAL=60
DEFAULT_MAX_PAGES=6
DEFAULT_PAGE_SIZE=20
DEFAULT_HOURS_LIMIT=2

is_running() {
  [[ -f "$PID_FILE" ]] && ps -p "$(cat "$PID_FILE")" >/dev/null 2>&1
}

start() {
  if is_running; then
    echo "$SERVICE_NAME is already running: $(cat "$PID_FILE")"
    exit 0
  fi

  if [[ -z "${PROCESS_MONITOR_BASE_URL:-}" ]]; then
    echo "PROCESS_MONITOR_BASE_URL is required" >&2
    exit 1
  fi

  cd "$PROJECT_ROOT"
  nohup env \
    PROCESS_MONITOR_BASE_URL="$PROCESS_MONITOR_BASE_URL" \
    PROCESS_MONITOR_TOKEN="${PROCESS_MONITOR_TOKEN:-}" \
    PROCESS_MONITOR_HOST="${PROCESS_MONITOR_HOST:-db002}" \
    TWITTER_API_BACKEND="${TWITTER_API_BACKEND:-twitterapi}" \
    "$PYTHON_BIN" "$PROJECT_ROOT/main.py" \
    --mode schedule \
    --interval "$DEFAULT_INTERVAL" \
    --max-pages "$DEFAULT_MAX_PAGES" \
    --page-size "$DEFAULT_PAGE_SIZE" \
    --hours-limit "$DEFAULT_HOURS_LIMIT" \
    --list-id "$LIST_ID" \
    --process-code "$PROCESS_CODE" \
    > "$LOG_FILE" 2>&1 &
  echo $! > "$PID_FILE"
  echo "$SERVICE_NAME started: $(cat "$PID_FILE")"
}

stop() {
  if is_running; then
    kill "$(cat "$PID_FILE")"
    rm -f "$PID_FILE"
    echo "$SERVICE_NAME stopped"
  else
    rm -f "$PID_FILE"
    echo "$SERVICE_NAME is not running"
  fi
}

status() {
  if is_running; then
    echo "$SERVICE_NAME running: $(cat "$PID_FILE")"
  else
    echo "$SERVICE_NAME stopped"
    exit 1
  fi
}

case "${1:-status}" in
  start) start ;;
  stop) stop ;;
  restart) stop; start ;;
  status) status ;;
  logs) tail -f "$LOG_FILE" ;;
  *) echo "Usage: $0 {start|stop|restart|status|logs}"; exit 2 ;;
esac
