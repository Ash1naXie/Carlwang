#!/bin/bash

# KOL推文数据爬取服务启动脚本（TweetScout临时版）
# 使用方法: ./start_service_kol_tweetscout.sh [start|stop|restart|status|once|logs]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$SCRIPT_DIR"

SERVICE_NAME="twitter-crawler-kol-tweetscout"
PID_FILE="$SCRIPT_DIR/${SERVICE_NAME}.pid"
LOG_FILE="$SCRIPT_DIR/service_kol_tweetscout.log"

DEFAULT_INTERVAL=60

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
print_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1"; }

check_status() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            print_info "KOL推文服务 (TweetScout) 正在运行 (PID: $PID)"
            return 0
        else
            print_warning "PID文件存在但进程不存在，清理PID文件"
            rm -f "$PID_FILE"
        fi
    fi
    print_info "KOL推文服务 (TweetScout) 未运行"
    return 1
}

stop_service() {
    print_info "正在停止KOL推文服务 (TweetScout)..."
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            kill "$PID" 2>/dev/null || true
            sleep 2
            if ps -p "$PID" > /dev/null 2>&1; then
                kill -9 "$PID" 2>/dev/null || true
            fi
        fi
        rm -f "$PID_FILE"
    fi
    print_success "KOL推文服务 (TweetScout) 已停止"
}

start_service() {
    local interval=${1:-$DEFAULT_INTERVAL}

    if check_status > /dev/null 2>&1; then
        print_error "KOL推文服务 (TweetScout) 已在运行"
        return 1
    fi

    print_info "启动KOL推文服务 (TweetScout)..."
    print_info "间隔: ${interval}分钟"

    mkdir -p "$(dirname "$LOG_FILE")"

    nohup bash -c "
        while true; do
            cd '$PROJECT_ROOT' && '$PROJECT_ROOT/venv/bin/python' '$SCRIPT_DIR/crawler_tweetscout.py'
            sleep $((interval * 60))
        done
    " > "$LOG_FILE" 2>&1 &

    local pid=$!
    echo $pid > "$PID_FILE"

    sleep 2
    if ps -p $pid > /dev/null 2>&1; then
        print_success "KOL推文服务 (TweetScout) 启动成功 (PID: $pid)"
        print_info "日志: $LOG_FILE"
    else
        print_error "启动失败"
        rm -f "$PID_FILE"
        return 1
    fi
}

run_once() {
    print_info "执行单次KOL推文爬取 (TweetScout)..."
    cd "$PROJECT_ROOT" && "$PROJECT_ROOT/venv/bin/python" "$SCRIPT_DIR/crawler_tweetscout.py" 2>&1 | tee -a "$LOG_FILE"
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        print_success "单次爬取完成"
    else
        print_error "单次爬取失败"
        return 1
    fi
}

show_logs() {
    local lines=${1:-50}
    if [ -f "$LOG_FILE" ]; then
        tail -n $lines "$LOG_FILE"
    else
        print_warning "日志文件不存在"
    fi
}

case "$1" in
    start) start_service "$2" ;;
    stop) stop_service ;;
    restart) stop_service; sleep 2; start_service "$2" ;;
    status) check_status ;;
    once) run_once ;;
    logs) show_logs "$2" ;;
    *)
        echo "用法: $0 {start|stop|restart|status|once|logs} [参数]"
        exit 1
        ;;
esac
