#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/home/centos/Project/Carlwang"
SERVICE_DIR="$ROOT_DIR/daily_predx_raw_tweet_crawler"
PID_FILE="$SERVICE_DIR/predx_raw_tweet.pid"
LOG_FILE="$SERVICE_DIR/predx_raw_tweet.log"
PYTHON_BIN="$ROOT_DIR/venv/bin/python"

LIST_IDS="${PREDX_RAW_LIST_IDS:-2076918265339920691,2076918496106352730,2076917145783660587,2089558588138430667}"
DEFAULT_INTERVAL="${PREDX_RAW_INTERVAL:-60}"
DEFAULT_MAX_PAGES="${PREDX_RAW_MAX_PAGES:-10}"
DEFAULT_PAGE_SIZE="${PREDX_RAW_PAGE_SIZE:-20}"
DEFAULT_HOURS_LIMIT="${PREDX_RAW_HOURS_LIMIT:-1}"
PROCESS_CODE="${PREDX_RAW_PROCESS_CODE:-db002_predx_raw_lists}"

is_running() {
  [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null
}

start() {
  mkdir -p "$SERVICE_DIR"
  if is_running; then
    echo "predx raw tweet crawler already running: pid=$(cat "$PID_FILE")"
    exit 0
  fi
  cd "$ROOT_DIR"
  nohup env TWITTER_API_BACKEND=twitterapi PREDX_RAW_LIST_IDS="$LIST_IDS" \
    "$PYTHON_BIN" main.py \
      --mode predx-raw-schedule \
      --interval "$DEFAULT_INTERVAL" \
      --max-pages "$DEFAULT_MAX_PAGES" \
      --page-size "$DEFAULT_PAGE_SIZE" \
      --hours-limit "$DEFAULT_HOURS_LIMIT" \
      --list-ids "$LIST_IDS" \
      --process-code "$PROCESS_CODE" \
    >> "$LOG_FILE" 2>&1 &
  echo $! > "$PID_FILE"
  echo "started predx raw tweet crawler: pid=$(cat "$PID_FILE") log=$LOG_FILE"
}

stop() {
  if is_running; then
    kill "$(cat "$PID_FILE")"
    echo "stopped predx raw tweet crawler: pid=$(cat "$PID_FILE")"
  else
    echo "predx raw tweet crawler not running"
  fi
  rm -f "$PID_FILE"
}

status() {
  if is_running; then
    echo "running pid=$(cat "$PID_FILE")"
  else
    echo "stopped"
  fi
}

case "${1:-status}" in
  start) start ;;
  stop) stop ;;
  restart) stop; start ;;
  status) status ;;
  *) echo "usage: $0 {start|stop|restart|status}"; exit 2 ;;
esac
