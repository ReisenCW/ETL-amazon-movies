#!/bin/bash

# 配置变量
readonly AIRFLOW_PORT=8080
readonly AIRFLOW_HOME="/home/cw/airflow"
readonly LOG_DIR="/home/cw/airflow/logs"
readonly SCRIPT_NAME=$(basename "$0")

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

check_airflow_installed() {
    if ! command -v airflow &> /dev/null; then
        log_error "Airflow 未安装或不在 PATH 中"
        exit 1
    fi
}

check_airflow_home() {
    if [ ! -d "$AIRFLOW_HOME" ]; then
        log_warn "Airflow 目录不存在: $AIRFLOW_HOME"
        log_info "请先运行: airflow db init"
    fi
}

start_airflow() {
    log_info "启动 Airflow..."
    
    check_airflow_installed
    check_airflow_home
    
    export AIRFLOW_HOME
    
    # 创建日志目录
    mkdir -p "$LOG_DIR"
    
    # 检查是否已运行
    if pgrep -f "airflow webserver" > /dev/null; then
        log_warn "Airflow webserver 已在运行"
    else
        log_info "启动 webserver..."
        airflow webserver -p $AIRFLOW_PORT -D \
            --pid "$AIRFLOW_HOME/webserver.pid" \
            --stdout "$LOG_DIR/webserver.out" \
            --stderr "$LOG_DIR/webserver.err" \
            --log-file "$LOG_DIR/webserver.log"
    fi
    
    if pgrep -f "airflow scheduler" > /dev/null; then
        log_warn "Airflow scheduler 已在运行"
    else
        log_info "启动 scheduler..."
        airflow scheduler -D \
            --pid "$AIRFLOW_HOME/scheduler.pid" \
            --stdout "$LOG_DIR/scheduler.out" \
            --stderr "$LOG_DIR/scheduler.err" \
            --log-file "$LOG_DIR/scheduler.log"
    fi
    
    log_info "Airflow 启动完成"
    log_info "Web 界面: http://localhost:$AIRFLOW_PORT"
}

stop_airflow() {
    log_info "停止 Airflow..."
    
    local pids=$(pgrep -f "airflow (scheduler|webserver)")
    
    if [ -z "$pids" ]; then
        log_info "未找到运行的 Airflow 进程"
        return 0
    fi
    
    log_info "找到进程: $pids"
    echo "$pids" | xargs -r kill -15
    
    # 等待进程结束
    local count=0
    while [ $count -lt 10 ]; do
        if ! pgrep -f "airflow (scheduler|webserver)" > /dev/null; then
            log_info "Airflow 已正常停止"
            break
        fi
        sleep 1
        ((count++))
    done
    
    # 强制终止残留进程
    local remaining_pids=$(pgrep -f "airflow (scheduler|webserver)")
    if [ -n "$remaining_pids" ]; then
        log_warn "强制终止进程: $remaining_pids"
        echo "$remaining_pids" | xargs -r kill -9
    fi
    
    # 清理pid文件
    rm -f "$AIRFLOW_HOME"/*.pid 2>/dev/null
    log_info "Airflow 停止完成"
}

show_status() {
    log_info "Airflow 进程状态:"
    
    local processes=$(pgrep -f "airflow (scheduler|webserver)")
    if [ -n "$processes" ]; then
        ps -p "$processes" -o pid,user,command
        log_info "Airflow 正在运行"
    else
        log_info "Airflow 未运行"
    fi
}

case $1 in
"start") start_airflow ;;
"stop") stop_airflow ;;
"status") show_status ;;
"restart")
    stop_airflow
    sleep 3
    start_airflow
    ;;
*)
    echo "用法: $SCRIPT_NAME {start|stop|status|restart}"
    exit 1
    ;;
esac