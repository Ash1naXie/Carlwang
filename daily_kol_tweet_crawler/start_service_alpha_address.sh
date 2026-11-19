#!/bin/bash

# alpha_address 竞对池抓取服务
# 使用方法: ./start_service_alpha_address.sh [start|stop|restart|status|once|logs|monitor|sync-pool|reindex|bootstrap]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BET_KOL_ROOT="$(cd "$PROJECT_ROOT/../bet_kol_link" && pwd)"
cd "$SCRIPT_DIR"

: "${PROCESS_MONITOR_BASE_URL:=https://luckycoin.ai}"
: "${PROCESS_MONITOR_HOST:=db002}"

SERVICE_NAME="twitter-crawler-alpha-address"
PID_FILE="$SCRIPT_DIR/${SERVICE_NAME}.pid"
LOG_FILE="$SCRIPT_DIR/service_alpha_address.log"
TARGET_LIST_ID="2045070679889055752"
TARGET_POOL_ID="alpha_address"
TARGET_LIST_NAME="alpha_address"
MAIN_PYTHON_ENV_VAR="CARLWANG_PYTHON_BIN"
POOL_PYTHON_ENV_VAR="BET_KOL_PYTHON_BIN"

EXPECTED_MODE="--list-id $TARGET_LIST_ID"

DEFAULT_INTERVAL=20
DEFAULT_MAX_PAGES=5
DEFAULT_PAGE_SIZE=20
DEFAULT_HOURS_LIMIT=0.5
DEFAULT_INDEX_HOURS_BACK=48

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

resolve_python_bin() {
    local env_var_name="$1"
    shift

    local explicit_path="${!env_var_name:-}"
    if [ -n "$explicit_path" ]; then
        if [ -x "$explicit_path" ]; then
            echo "$explicit_path"
            return 0
        fi
        print_error "$env_var_name 指向的 Python 不可执行: $explicit_path"
        return 1
    fi

    local candidate
    for candidate in "$@"; do
        if [ -n "$candidate" ] && [ -x "$candidate" ]; then
            echo "$candidate"
            return 0
        fi
    done

    if command -v python3 >/dev/null 2>&1; then
        command -v python3
        return 0
    fi

    print_error "没找到可用 Python，请先 export $env_var_name=/绝对路径/python"
    return 1
}

get_main_python_bin() {
    resolve_python_bin \
        "$MAIN_PYTHON_ENV_VAR" \
        "$PROJECT_ROOT/venv/bin/python" \
        "$PROJECT_ROOT/.venv/bin/python"
}

get_pool_python_bin() {
    local main_python="${1:-}"
    resolve_python_bin \
        "$POOL_PYTHON_ENV_VAR" \
        "$BET_KOL_ROOT/venv/bin/python" \
        "$BET_KOL_ROOT/.venv/bin/python" \
        "$main_python"
}

verify_main_runtime() {
    local py_bin="$1"
    PROJECT_ROOT="$PROJECT_ROOT" "$py_bin" <<'PY'
import os
import sys
from pathlib import Path

project_root = Path(os.environ["PROJECT_ROOT"])
sys.path.insert(0, str(project_root))

try:
    import main  # noqa: F401
except Exception as exc:
    print(f"ERR|{type(exc).__name__}|{exc}")
    raise SystemExit(1)
PY
}

verify_pool_runtime() {
    local py_bin="$1"
    BET_KOL_ROOT="$BET_KOL_ROOT" "$py_bin" <<'PY'
import os
import sys
from pathlib import Path

repo_root = Path(os.environ["BET_KOL_ROOT"]).parent
sys.path.insert(0, str(repo_root))

for target in (
    "bet_kol_link.scripts.sync_x_list_to_pool",
    "bet_kol_link.scripts.backfill_kol_pool_tweets",
):
    try:
        __import__(target)
    except Exception as exc:
        print(f"ERR|{target}|{type(exc).__name__}|{exc}")
        raise SystemExit(1)
PY
}

require_main_runtime() {
    local py_bin
    local verify_output
    py_bin=$(get_main_python_bin) || return 1
    verify_output=$(verify_main_runtime "$py_bin" 2>&1) || {
        if [ -n "$verify_output" ]; then
            printf '%s\n' "$verify_output" >&2
        fi
        print_error "Carlwang 运行环境不完整，请先安装依赖，或 export $MAIN_PYTHON_ENV_VAR=/绝对路径/python"
        return 1
    }
    echo "$py_bin"
}

require_pool_runtime() {
    local main_python="${1:-}"
    local py_bin
    local verify_output
    py_bin=$(get_pool_python_bin "$main_python") || return 1
    verify_output=$(verify_pool_runtime "$py_bin" 2>&1) || {
        if [ -n "$verify_output" ]; then
            printf '%s\n' "$verify_output" >&2
        fi
        print_error "bet_kol_link 运行环境不完整，请先安装依赖，或 export $POOL_PYTHON_ENV_VAR=/绝对路径/python"
        return 1
    }
    echo "$py_bin"
}

build_post_crawl_hook() {
    local pool_python="$1"
    local hours_back="${2:-$DEFAULT_INDEX_HOURS_BACK}"
    echo "'$pool_python' '$BET_KOL_ROOT/scripts/backfill_kol_pool_tweets.py' --pool-id '$TARGET_POOL_ID' --hours-back '$hours_back'"
}

check_status() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            CMDLINE=$(ps -p "$PID" -o command= 2>/dev/null)
            if [[ "$CMDLINE" == *"$PROJECT_ROOT/main.py"* ]] && [[ "$CMDLINE" == *"$EXPECTED_MODE"* ]]; then
                print_info "alpha_address 抓取服务正在运行 (PID: $PID)"
                return 0
            fi
            print_warning "PID文件指向的进程不是预期 alpha_address 爬虫，清理PID文件"
            print_warning "当前进程命令: $CMDLINE"
            rm -f "$PID_FILE"
        else
            print_warning "PID文件存在但进程不存在，清理PID文件"
            rm -f "$PID_FILE"
        fi
    fi

    RUNNING_LINES=$(ps -ef | grep -E "[Pp]ython.*$PROJECT_ROOT/.*main.py" | grep -F -- "$EXPECTED_MODE" 2>/dev/null || true)
    if [ -n "$RUNNING_LINES" ]; then
        RUNNING_PROCS=$(echo "$RUNNING_LINES" | wc -l | tr -d ' ')
        print_warning "发现 $RUNNING_PROCS 个运行中的 alpha_address 爬虫进程（无PID文件追踪）"
        echo "$RUNNING_LINES" | while read -r line; do
            ORPHAN_PID=$(echo "$line" | awk '{print $2}')
            START_TIME=$(echo "$line" | awk '{print $5}')
            print_warning "  - PID $ORPHAN_PID (启动时间: $START_TIME)"
        done
        return 0
    fi

    print_info "alpha_address 抓取服务未运行"
    return 1
}

sync_pool() {
    local pool_python
    pool_python=$(require_pool_runtime) || return 1
    print_info "同步 alpha_address 池成员..."
    cd "$BET_KOL_ROOT" && "$pool_python" "$BET_KOL_ROOT/scripts/sync_x_list_to_pool.py" \
        --list-id "$TARGET_LIST_ID" \
        --pool-id "$TARGET_POOL_ID" \
        --list-name "$TARGET_LIST_NAME" \
        --primary-category competitor \
        --source x_list \
        --selection-reason "list_id=$TARGET_LIST_ID"
}

reindex_pool() {
    local hours_back=${1:-$DEFAULT_INDEX_HOURS_BACK}
    local pool_python
    pool_python=$(require_pool_runtime) || return 1
    print_info "回填 alpha_address 池推文索引，窗口=${hours_back}小时..."
    cd "$BET_KOL_ROOT" && "$pool_python" "$BET_KOL_ROOT/scripts/backfill_kol_pool_tweets.py" \
        --pool-id "$TARGET_POOL_ID" \
        --hours-back "$hours_back"
}

stop_service() {
    print_info "正在停止 alpha_address 抓取服务..."

    pkill -f "$PROJECT_ROOT/.*main.py.*$EXPECTED_MODE" 2>/dev/null || true

    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            kill "$PID" 2>/dev/null || true
            sleep 3
            if ps -p "$PID" > /dev/null 2>&1; then
                print_warning "进程未响应，强制终止"
                kill -9 "$PID" 2>/dev/null || true
            fi
        fi
        pkill -f "caffeinate.*-w[[:space:]]*$PID" 2>/dev/null || true
        rm -f "$PID_FILE"
    fi

    remove_monitoring
    print_success "alpha_address 抓取服务已停止"
}

start_service() {
    local interval=${1:-$DEFAULT_INTERVAL}
    local max_pages=${2:-$DEFAULT_MAX_PAGES}
    local page_size=${3:-$DEFAULT_PAGE_SIZE}
    local hours_limit=${4:-$DEFAULT_HOURS_LIMIT}
    local index_hours_back=${5:-$DEFAULT_INDEX_HOURS_BACK}
    local main_python
    local pool_python
    local hook_cmd

    if check_status > /dev/null 2>&1; then
        print_error "alpha_address 抓取服务已在运行，请先停止服务"
        return 1
    fi

    main_python=$(require_main_runtime) || return 1
    pool_python=$(require_pool_runtime "$main_python") || return 1

    print_info "启动前同步池成员..."
    if ! sync_pool; then
        print_error "池成员同步失败，拒绝启动"
        return 1
    fi

    print_info "清理Python字节码缓存..."
    find "$PROJECT_ROOT" -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null
    find "$PROJECT_ROOT" -name "*.pyc" -delete 2>/dev/null

    print_info "启动 alpha_address 抓取服务..."
    print_info "配置: ListID=${TARGET_LIST_ID}, pool=${TARGET_POOL_ID}, 间隔=${interval}分钟, 页数=${max_pages}, 时间限制=${hours_limit}小时"

    mkdir -p "$(dirname "$LOG_FILE")"
    hook_cmd=$(build_post_crawl_hook "$pool_python" "$index_hours_back")

    if [[ -z "${PROCESS_MONITOR_BASE_URL:-}" ]]; then
        print_error "PROCESS_MONITOR_BASE_URL 未配置"
        return 1
    fi

    if [[ "$OSTYPE" == "darwin"* ]]; then
        print_info "检测到macOS，使用防休眠启动..."
        POST_CRAWL_CMD="$hook_cmd" PROCESS_MONITOR_BASE_URL="$PROCESS_MONITOR_BASE_URL" PROCESS_MONITOR_TOKEN="${PROCESS_MONITOR_TOKEN:-}" PROCESS_MONITOR_HOST="${PROCESS_MONITOR_HOST:-db002}" TWITTER_API_BACKEND=twitterapi nohup bash -c "cd '$PROJECT_ROOT' && exec '$main_python' '$PROJECT_ROOT/main.py' --mode schedule \
            --interval $interval \
            --max-pages $max_pages \
            --page-size $page_size \
            --hours-limit $hours_limit \
            --list-id $TARGET_LIST_ID \
            --process-code db002_alpha_address" > "$LOG_FILE" 2>&1 &
    else
        POST_CRAWL_CMD="$hook_cmd" PROCESS_MONITOR_BASE_URL="$PROCESS_MONITOR_BASE_URL" PROCESS_MONITOR_TOKEN="${PROCESS_MONITOR_TOKEN:-}" PROCESS_MONITOR_HOST="${PROCESS_MONITOR_HOST:-db002}" TWITTER_API_BACKEND=twitterapi nohup nice -n -5 bash -c "cd '$PROJECT_ROOT' && exec '$main_python' '$PROJECT_ROOT/main.py' --mode schedule \
            --interval $interval \
            --max-pages $max_pages \
            --page-size $page_size \
            --hours-limit $hours_limit \
            --list-id $TARGET_LIST_ID \
            --process-code db002_alpha_address" > "$LOG_FILE" 2>&1 &
    fi

    local pid=$!
    echo $pid > "$PID_FILE"

    if [[ -z "${PROCESS_MONITOR_BASE_URL:-}" ]]; then
        print_error "PROCESS_MONITOR_BASE_URL 未配置"
        return 1
    fi

    if [[ "$OSTYPE" == "darwin"* ]]; then
        caffeinate -i -w "$pid" >/dev/null 2>&1 &
    fi

    sleep 3
    if ps -p $pid > /dev/null 2>&1; then
        print_success "alpha_address 抓取服务启动成功 (PID: $pid)"
        print_info "日志文件: $LOG_FILE"
        print_info "PID文件: $PID_FILE"
        setup_monitoring
    else
        print_error "alpha_address 抓取服务启动失败"
        rm -f "$PID_FILE"
        return 1
    fi
}

setup_monitoring() {
    local monitor_script="$SCRIPT_DIR/service_alpha_address_monitor.sh"
    if [ ! -f "$monitor_script" ]; then
        print_warning "监控脚本不存在: $monitor_script"
        return 1
    fi

    local cron_entry="*/5 * * * * $monitor_script >/dev/null 2>&1"
    local existing_cron=$(crontab -l 2>/dev/null | grep -F "$monitor_script" | head -1)
    if [ -n "$existing_cron" ]; then
        print_info "alpha_address 监控定时任务已存在: $existing_cron"
        return 0
    fi

    print_info "设置 alpha_address 监控定时任务 (每5分钟检查一次)..."
    local temp_cron=$(mktemp)
    crontab -l 2>/dev/null > "$temp_cron"
    echo "# alpha_address crawler monitor" >> "$temp_cron"
    echo "$cron_entry" >> "$temp_cron"
    if crontab "$temp_cron" 2>/dev/null; then
        print_success "alpha_address 监控定时任务设置成功"
    else
        print_error "设置 alpha_address 监控定时任务失败"
        print_info "$cron_entry"
    fi
    rm -f "$temp_cron"
}

remove_monitoring() {
    local monitor_script="$SCRIPT_DIR/service_alpha_address_monitor.sh"
    local temp_cron=$(mktemp)
    if crontab -l 2>/dev/null > "$temp_cron"; then
        grep -vF "$monitor_script" "$temp_cron" > "$temp_cron.new"
        if ! cmp -s "$temp_cron" "$temp_cron.new"; then
            if crontab "$temp_cron.new" 2>/dev/null; then
                print_info "alpha_address 监控定时任务已移除"
            fi
        fi
        rm -f "$temp_cron.new"
    fi
    rm -f "$temp_cron"
}

restart_service() {
    print_info "重启 alpha_address 抓取服务..."
    stop_service
    sleep 2
    start_service "$@"
}

show_logs() {
    local lines=${1:-50}
    if [ -f "$LOG_FILE" ]; then
        print_info "显示最新 $lines 行日志:"
        echo "----------------------------------------"
        tail -n $lines "$LOG_FILE"
    else
        print_warning "日志文件不存在"
    fi
}

run_once() {
    local max_pages=${1:-$DEFAULT_MAX_PAGES}
    local page_size=${2:-$DEFAULT_PAGE_SIZE}
    local hours_limit=${3:-$DEFAULT_HOURS_LIMIT}
    local index_hours_back=${4:-$DEFAULT_INDEX_HOURS_BACK}
    local main_python
    local pool_python
    local hook_cmd

    main_python=$(require_main_runtime) || return 1
    pool_python=$(require_pool_runtime "$main_python") || return 1

    print_info "开始执行单次 alpha_address 抓取..."
    print_info "配置: ListID=${TARGET_LIST_ID}, 页数=${max_pages}, 时间限制=${hours_limit}小时"

    if ! sync_pool; then
        print_error "池成员同步失败"
        return 1
    fi

    mkdir -p "$(dirname "$LOG_FILE")"
    hook_cmd=$(build_post_crawl_hook "$pool_python" "$index_hours_back")

    cd "$PROJECT_ROOT" && POST_CRAWL_CMD="$hook_cmd" TWITTER_API_BACKEND=twitterapi "$main_python" "$PROJECT_ROOT/main.py" --mode once \
        --max-pages $max_pages \
        --page-size $page_size \
        --hours-limit $hours_limit \
        --list-id $TARGET_LIST_ID \
        --process-code db002_alpha_address 2>&1 | tee -a "$LOG_FILE"

    local exit_code=${PIPESTATUS[0]}
    if [ $exit_code -eq 0 ]; then
        print_success "单次抓取完成"
    else
        print_error "单次抓取失败 (退出码: $exit_code)"
        return 1
    fi
}

show_monitor() {
    local monitor_script="$SCRIPT_DIR/service_alpha_address_monitor.sh"
    local monitor_log="$SCRIPT_DIR/monitor_alpha_address.log"

    print_info "=== alpha_address 监控状态 ==="
    local existing_cron=$(crontab -l 2>/dev/null | grep -F "$monitor_script")
    if [ -n "$existing_cron" ]; then
        print_success "监控定时任务: 已启用"
        echo "  $existing_cron"
    else
        print_warning "监控定时任务: 未启用"
    fi

    if [ -f "$monitor_log" ]; then
        print_info "最近监控日志 (最新10行):"
        echo "----------------------------------------"
        tail -n 10 "$monitor_log"
    else
        print_info "监控日志: 无记录"
    fi
}

bootstrap_all() {
    local max_pages=${1:-$DEFAULT_MAX_PAGES}
    local page_size=${2:-$DEFAULT_PAGE_SIZE}
    local hours_limit=${3:-$DEFAULT_HOURS_LIMIT}
    local index_hours_back=${4:-$DEFAULT_INDEX_HOURS_BACK}

    print_info "执行 alpha_address 初始化..."
    run_once "$max_pages" "$page_size" "$hours_limit" "$index_hours_back"
}

show_help() {
    echo "alpha_address 竞对池抓取服务"
    echo ""
    echo "使用方法:"
    echo "  $0 [命令] [参数]"
    echo ""
    echo "命令:"
    echo "  start [间隔] [页数] [每页条数] [小时限制] [回填窗口小时]"
    echo "  stop"
    echo "  restart [间隔] [页数] [每页条数] [小时限制] [回填窗口小时]"
    echo "  status"
    echo "  once [页数] [每页条数] [小时限制] [回填窗口小时]"
    echo "  logs [行数]"
    echo "  monitor"
    echo "  sync-pool"
    echo "  reindex [回填窗口小时]"
    echo "  bootstrap [页数] [每页条数] [小时限制] [回填窗口小时]"
    echo ""
    echo "固定参数:"
    echo "  list_id: $TARGET_LIST_ID"
    echo "  pool_id: $TARGET_POOL_ID"
    echo "  list_name: $TARGET_LIST_NAME"
}

case "$1" in
    "start")
        start_service "$2" "$3" "$4" "$5" "$6"
        ;;
    "stop")
        stop_service
        ;;
    "restart")
        restart_service "$2" "$3" "$4" "$5" "$6"
        ;;
    "status")
        check_status
        if [ -f "$LOG_FILE" ]; then
            print_info "最新日志:"
            echo "----------------------------------------"
            tail -n 5 "$LOG_FILE"
        fi
        ;;
    "once")
        run_once "$2" "$3" "$4" "$5"
        ;;
    "logs")
        show_logs "$2"
        ;;
    "monitor")
        show_monitor
        ;;
    "sync-pool")
        sync_pool
        ;;
    "reindex")
        reindex_pool "$2"
        ;;
    "bootstrap")
        bootstrap_all "$2" "$3" "$4" "$5"
        ;;
    "help"|"-h"|"--help"|"")
        show_help
        ;;
    *)
        print_error "未知命令: $1"
        exit 1
        ;;
esac
