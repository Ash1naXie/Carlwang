#!/bin/bash

# alpha_address 抓取服务监控脚本

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_SCRIPT="$SCRIPT_DIR/start_service_alpha_address.sh"
PID_FILE="$SCRIPT_DIR/twitter-crawler-alpha-address.pid"
MONITOR_LOG="$SCRIPT_DIR/monitor_alpha_address.log"

PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
EXPECTED_MODE="--list-id 2045070679889055752"

log_monitor() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$MONITOR_LOG"
}

check_service() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            return 0
        fi
    fi

    RUNNING_PROCS=$(ps -ef | grep -E "[Pp]ython.*$PROJECT_ROOT/.*main.py" | grep -F -- "$EXPECTED_MODE" | wc -l | tr -d ' ')
    if [ "$RUNNING_PROCS" -gt 0 ]; then
        return 0
    fi

    return 1
}

if check_service; then
    log_monitor "✓ alpha_address 抓取服务运行正常"
else
    log_monitor "✗ alpha_address 抓取服务已停止，尝试重启..."

    if [ -f "$SERVICE_SCRIPT" ]; then
        "$SERVICE_SCRIPT" start >> "$MONITOR_LOG" 2>&1
        sleep 5

        if check_service; then
            log_monitor "✓ alpha_address 抓取服务重启成功"
        else
            log_monitor "✗ alpha_address 抓取服务重启失败"
        fi
    else
        log_monitor "✗ 服务管理脚本不存在: $SERVICE_SCRIPT"
    fi
fi

if [ -f "$MONITOR_LOG" ]; then
    LINE_COUNT=$(wc -l < "$MONITOR_LOG")
    if [ $LINE_COUNT -gt 1000 ]; then
        tail -n 1000 "$MONITOR_LOG" > "$MONITOR_LOG.tmp"
        mv "$MONITOR_LOG.tmp" "$MONITOR_LOG"
    fi
fi
