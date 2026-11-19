#!/bin/bash

# KOL推文数据爬取服务监控脚本（增强版）
# 由 cron 定时运行，采用「进程 + 日志心跳 + DB更新时间」联合判定，并支持自动重启

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_SCRIPT="$SCRIPT_DIR/start_service_kol_tweet.sh"
PID_FILE="$SCRIPT_DIR/twitter-crawler-kol-tweet.pid"
SERVICE_LOG="$SCRIPT_DIR/service_kol_tweet.log"
MONITOR_LOG="$SCRIPT_DIR/monitor_kol_tweet.log"
STATE_FILE="$SCRIPT_DIR/.monitor_kol_tweet.state"

PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
EXPECTED_MODE="--mode schedule"

# 运行参数（可通过环境变量覆盖）
MONITOR_INTERVAL_MIN=${MONITOR_INTERVAL_MIN:-5}
LOG_STALE_THRESHOLD_MIN=${LOG_STALE_THRESHOLD_MIN:-30}
FALLBACK_LOG_STALE_THRESHOLD_MIN=${FALLBACK_LOG_STALE_THRESHOLD_MIN:-90}
REQUIRED_CONSECUTIVE_UNHEALTHY=${REQUIRED_CONSECUTIVE_UNHEALTHY:-2}
RESTART_COOLDOWN_MIN=${RESTART_COOLDOWN_MIN:-30}
MAX_RESTARTS_PER_DAY=${MAX_RESTARTS_PER_DAY:-6}

# DB阈值使用动态值，并做上下限保护
DB_STALE_MIN_FLOOR=${DB_STALE_MIN_FLOOR:-120}
DB_STALE_MIN_CEIL=${DB_STALE_MIN_CEIL:-360}

# 调试模式：MONITOR_DRY_RUN=1 时只打日志不执行重启
MONITOR_DRY_RUN=${MONITOR_DRY_RUN:-0}


log_monitor() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$MONITOR_LOG"
}


trim_monitor_log() {
    if [ -f "$MONITOR_LOG" ]; then
        local line_count
        line_count=$(wc -l < "$MONITOR_LOG")
        if [ "$line_count" -gt 1000 ]; then
            tail -n 1000 "$MONITOR_LOG" > "$MONITOR_LOG.tmp"
            mv "$MONITOR_LOG.tmp" "$MONITOR_LOG"
        fi
    fi
}


acquire_lock() {
    LOCK_DIR="/tmp/twitter-crawler-kol-tweet-monitor.lock"
    if ! mkdir "$LOCK_DIR" 2>/dev/null; then
        # 另一个监控实例正在运行
        exit 0
    fi
    trap 'rmdir "$LOCK_DIR" 2>/dev/null || true' EXIT
}


load_state() {
    state_consecutive_unhealthy=0
    state_last_restart_epoch=0
    state_restart_count_date=""
    state_restart_count_today=0

    if [ -f "$STATE_FILE" ]; then
        # shellcheck disable=SC1090
        source "$STATE_FILE" 2>/dev/null || true
    fi

    local today
    today=$(date '+%Y-%m-%d')
    if [ "$state_restart_count_date" != "$today" ]; then
        state_restart_count_date="$today"
        state_restart_count_today=0
    fi
}


save_state() {
    cat > "$STATE_FILE" <<EOF
state_consecutive_unhealthy=$state_consecutive_unhealthy
state_last_restart_epoch=$state_last_restart_epoch
state_restart_count_date=$state_restart_count_date
state_restart_count_today=$state_restart_count_today
EOF
}


is_expected_service_cmd() {
    local pid="$1"
    local cmdline
    cmdline=$(ps -p "$pid" -o command= 2>/dev/null)

    # 仅识别本项目的 KOL schedule 进程，排除重点KOL（带 --list-id）
    if [[ "$cmdline" == *"$PROJECT_ROOT/main.py"* ]] && [[ "$cmdline" == *"$EXPECTED_MODE"* ]] && [[ "$cmdline" != *"--list-id"* ]]; then
        return 0
    fi

    return 1
}


find_running_pid_from_process_list() {
    ps -eo pid=,command= 2>/dev/null | \
        grep -F "$PROJECT_ROOT/main.py" | \
        grep -F -- "$EXPECTED_MODE" | \
        grep -v -- "--list-id" | \
        awk 'NR==1{print $1}'
}


resolve_service_pid() {
    local pid=""

    # 优先使用PID文件
    if [ -f "$PID_FILE" ]; then
        pid=$(cat "$PID_FILE" 2>/dev/null)
        if [ -n "$pid" ] && ps -p "$pid" >/dev/null 2>&1 && is_expected_service_cmd "$pid"; then
            echo "$pid"
            return 0
        fi
    fi

    # 兜底从进程列表查
    pid=$(find_running_pid_from_process_list)
    if [ -n "$pid" ] && ps -p "$pid" >/dev/null 2>&1 && is_expected_service_cmd "$pid"; then
        echo "$pid"
        return 0
    fi

    return 1
}


check_service() {
    SERVICE_PID="$(resolve_service_pid 2>/dev/null || true)"
    if [ -n "$SERVICE_PID" ]; then
        return 0
    fi
    return 1
}


get_runtime_interval_minutes() {
    local pid="$1"
    local default_interval=60
    local cmdline interval

    cmdline=$(ps -p "$pid" -o command= 2>/dev/null)
    interval=$(echo "$cmdline" | sed -n 's/.*--interval[[:space:]]\([0-9][0-9]*\).*/\1/p' | head -1)

    if [[ "$interval" =~ ^[0-9]+$ ]] && [ "$interval" -gt 0 ]; then
        echo "$interval"
    else
        echo "$default_interval"
    fi
}


calculate_dynamic_db_threshold() {
    local interval_min="$1"
    local threshold=$((interval_min * 2 + 60))

    if [ "$threshold" -lt "$DB_STALE_MIN_FLOOR" ]; then
        threshold="$DB_STALE_MIN_FLOOR"
    fi
    if [ "$threshold" -gt "$DB_STALE_MIN_CEIL" ]; then
        threshold="$DB_STALE_MIN_CEIL"
    fi

    echo "$threshold"
}


get_file_mtime_epoch() {
    local file_path="$1"
    local mtime=""

    # Linux
    mtime=$(stat -c %Y "$file_path" 2>/dev/null || true)
    if [[ "$mtime" =~ ^[0-9]+$ ]]; then
        echo "$mtime"
        return 0
    fi

    # macOS
    mtime=$(stat -f %m "$file_path" 2>/dev/null || true)
    if [[ "$mtime" =~ ^[0-9]+$ ]]; then
        echo "$mtime"
        return 0
    fi

    return 1
}


get_service_log_age_minutes() {
    if [ ! -f "$SERVICE_LOG" ]; then
        echo "-1"
        return 0
    fi

    local mtime now age
    mtime=$(get_file_mtime_epoch "$SERVICE_LOG" 2>/dev/null || true)
    if ! [[ "$mtime" =~ ^[0-9]+$ ]]; then
        echo "-1"
        return 0
    fi

    now=$(date +%s)
    age=$(( (now - mtime) / 60 ))
    if [ "$age" -lt 0 ]; then
        age=0
    fi
    echo "$age"
}


get_python_bin() {
    if [ -x "$PROJECT_ROOT/venv/bin/python" ]; then
        echo "$PROJECT_ROOT/venv/bin/python"
        return 0
    fi
    if command -v python3 >/dev/null 2>&1; then
        command -v python3
        return 0
    fi
    return 1
}


get_db_update_age() {
    local py_bin
    py_bin=$(get_python_bin 2>/dev/null || true)

    if [ -z "$py_bin" ]; then
        echo "ERR|python_not_found"
        return 0
    fi

    PROJECT_ROOT="$PROJECT_ROOT" "$py_bin" <<'PY' 2>/dev/null
import json
import os
import re
from pathlib import Path

try:
    import pymysql
except Exception as exc:
    print(f"ERR|pymysql_import_failed:{type(exc).__name__}")
    raise SystemExit(0)

project_root = Path(os.environ.get("PROJECT_ROOT", ""))
config_path = project_root / "config" / "config.json"

try:
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
except Exception as exc:
    print(f"ERR|config_load_failed:{type(exc).__name__}")
    raise SystemExit(0)

db_cfg = cfg.get("database", {})
table_name = os.getenv("MONITOR_DB_TABLE") or db_cfg.get("tables", {}).get("tweet", "twitter_tweet")

if not re.match(r"^[A-Za-z0-9_]+$", table_name or ""):
    print("ERR|invalid_table_name")
    raise SystemExit(0)

host = os.getenv("MONITOR_DB_HOST") or db_cfg.get("host", "")
port = int(os.getenv("MONITOR_DB_PORT") or db_cfg.get("port", 3306))
user = os.getenv("MONITOR_DB_USER") or db_cfg.get("username", "")
password = os.getenv("MONITOR_DB_PASSWORD") or db_cfg.get("password", "")
database = os.getenv("MONITOR_DB_NAME") or db_cfg.get("database", "")

if not all([host, user, database]):
    print("ERR|db_config_incomplete")
    raise SystemExit(0)

conn = None
try:
    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        connect_timeout=8,
        read_timeout=8,
        write_timeout=8,
    )

    with conn.cursor() as cur:
        cur.execute(f"SELECT NOW(), MAX(update_time) FROM {table_name}")
        now_ts, max_update = cur.fetchone()

    if max_update is None:
        print(f"OK|999999|NULL|{table_name}")
    else:
        age_min = (now_ts - max_update).total_seconds() / 60.0
        print(f"OK|{age_min:.1f}|{max_update}|{table_name}")

except Exception as exc:
    print(f"ERR|db_query_failed:{type(exc).__name__}")
finally:
    try:
        if conn:
            conn.close()
    except Exception:
        pass
PY
}


attempt_recovery() {
    local reason="$1"
    local action="$2"
    local now_epoch
    now_epoch=$(date +%s)

    if [ "$state_restart_count_today" -ge "$MAX_RESTARTS_PER_DAY" ]; then
        log_monitor "✗ 达到单日最大重启次数(${MAX_RESTARTS_PER_DAY})，跳过重启。原因: ${reason}"
        return 1
    fi

    local minutes_since_last_restart
    minutes_since_last_restart=$(( (now_epoch - state_last_restart_epoch) / 60 ))
    if [ "$state_last_restart_epoch" -gt 0 ] && [ "$minutes_since_last_restart" -lt "$RESTART_COOLDOWN_MIN" ]; then
        log_monitor "⚠️ 重启冷却中（距上次重启 ${minutes_since_last_restart} 分钟 < ${RESTART_COOLDOWN_MIN} 分钟），跳过重启。原因: ${reason}"
        return 1
    fi

    log_monitor "✗ 判定服务异常，执行 ${action}。原因: ${reason}"

    if [ "$MONITOR_DRY_RUN" = "1" ]; then
        log_monitor "🧪 DRY_RUN=1，仅演练不真正执行重启"
        return 0
    fi

    if [ ! -f "$SERVICE_SCRIPT" ]; then
        log_monitor "✗ 服务管理脚本不存在: $SERVICE_SCRIPT"
        return 1
    fi

    "$SERVICE_SCRIPT" "$action" >> "$MONITOR_LOG" 2>&1
    sleep 8

    state_last_restart_epoch="$now_epoch"
    state_restart_count_today=$((state_restart_count_today + 1))

    if check_service; then
        state_consecutive_unhealthy=0
        log_monitor "✓ KOL推文爬取服务${action}成功 (PID: ${SERVICE_PID})"
        return 0
    else
        log_monitor "✗ KOL推文爬取服务${action}失败"
        return 1
    fi
}


main() {
    acquire_lock
    load_state

    local now_epoch unhealthy reason action
    now_epoch=$(date +%s)
    unhealthy=0
    reason=""
    action="restart"

    if ! check_service; then
        unhealthy=1
        reason="process_not_running"
        action="start"
    else
        local interval_min db_stale_threshold log_age
        interval_min=$(get_runtime_interval_minutes "$SERVICE_PID")
        db_stale_threshold=$(calculate_dynamic_db_threshold "$interval_min")
        log_age=$(get_service_log_age_minutes)

        if [ "$log_age" -lt 0 ]; then
            log_age=999999
        fi

        if [ "$log_age" -le "$LOG_STALE_THRESHOLD_MIN" ]; then
            unhealthy=0
            reason="healthy"
            log_monitor "✓ 服务正常 (PID=${SERVICE_PID}, interval=${interval_min}m, log_age=${log_age}m)"
        else
            # 日志心跳变慢时，再结合DB最新更新时间进行二次判定
            local db_result db_status db_age_raw db_time db_table db_age_int
            db_result=$(get_db_update_age)
            db_status=$(echo "$db_result" | awk -F'|' '{print $1}')

            if [ "$db_status" = "OK" ]; then
                db_age_raw=$(echo "$db_result" | awk -F'|' '{print $2}')
                db_time=$(echo "$db_result" | awk -F'|' '{print $3}')
                db_table=$(echo "$db_result" | awk -F'|' '{print $4}')

                if [[ "$db_age_raw" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
                    db_age_int=$(printf '%.0f' "$db_age_raw")
                else
                    db_age_int=999999
                fi

                if [ "$db_age_int" -ge "$db_stale_threshold" ]; then
                    unhealthy=1
                    reason="log_stale(${log_age}m>${LOG_STALE_THRESHOLD_MIN}m)+db_stale(${db_age_int}m>=${db_stale_threshold}m,table=${db_table},max=${db_time})"
                else
                    unhealthy=0
                    reason="log_stale_but_db_fresh(log=${log_age}m,db=${db_age_int}m<th=${db_stale_threshold}m)"
                    log_monitor "⚠️ 日志心跳偏慢，但DB仍在更新，暂不重启 (${reason})"
                fi
            else
                # DB检查失败时，采用日志兜底阈值
                if [ "$log_age" -ge "$FALLBACK_LOG_STALE_THRESHOLD_MIN" ]; then
                    unhealthy=1
                    reason="log_stale_fallback(${log_age}m>=${FALLBACK_LOG_STALE_THRESHOLD_MIN}m)+db_check_failed(${db_result})"
                else
                    unhealthy=0
                    reason="db_check_failed_but_log_not_too_stale(${log_age}m<${FALLBACK_LOG_STALE_THRESHOLD_MIN}m)"
                    log_monitor "⚠️ DB检查失败，但日志未超过兜底阈值，暂不重启 (${db_result})"
                fi
            fi
        fi
    fi

    if [ "$unhealthy" -eq 1 ]; then
        state_consecutive_unhealthy=$((state_consecutive_unhealthy + 1))
        log_monitor "⚠️ 异常计数: ${state_consecutive_unhealthy}/${REQUIRED_CONSECUTIVE_UNHEALTHY}，原因: ${reason}"

        if [ "$state_consecutive_unhealthy" -ge "$REQUIRED_CONSECUTIVE_UNHEALTHY" ]; then
            attempt_recovery "$reason" "$action" || true
        fi
    else
        if [ "$state_consecutive_unhealthy" -gt 0 ]; then
            log_monitor "✓ 服务恢复健康，异常计数清零（此前=${state_consecutive_unhealthy}）"
        fi
        state_consecutive_unhealthy=0
    fi

    save_state
    trim_monitor_log
}


main
