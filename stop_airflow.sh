#!/bin/bash
# Tắt Airflow CỦA DỰ ÁN NÀY: scheduler, dag-processor, triggerer, web :8080, worker.
# Chạy: ./stop_airflow.sh            — tắt
#       ./stop_airflow.sh --dry-run  — chỉ liệt kê tiến trình sẽ tắt
#
# Vì sao không dùng `pkill -f .../airflow_venv/bin/airflow`: tiến trình con của Airflow tự
# đổi tên (setproctitle) thành "airflow api_server -- host:0.0.0.0 port:8080",
# "airflow scheduler", "worker -- LocalExecutor"... — mất đường dẫn venv nên pkill không
# khớp. Máy 192.169.1.168, 2026-09-30: pkill xong vẫn còn api_server mồ côi giữ cổng
# 8080 từ 14:46 hôm trước, qua mấy lần khởi động lại web vẫn là tiến trình cũ.
# Nhận diện: đường dẫn venv trong lệnh, HOẶC tên kiểu Airflow + thư mục làm việc (cwd) =
# thư mục dự án (start_airflow.sh cd vào đó) — không đụng Airflow của dự án/máy khác.

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${VENV_DIR:-$PROJECT_DIR/airflow_venv}"
DRY_RUN=0; [ "$1" = "--dry-run" ] && DRY_RUN=1

find_pids() {
  local d pid cmd
  for d in /proc/[0-9]*; do
    pid=${d#/proc/}
    # Bỏ chính script này và tiến trình gọi nó (start_airflow.sh gọi stop trước khi chạy)
    [ "$pid" = "$$" ] || [ "$pid" = "$PPID" ] && continue
    [ -O "$d" ] || continue                              # chỉ tiến trình của user này
    cmd=$(tr '\0' ' ' < "$d/cmdline" 2>/dev/null) || continue
    case "$cmd" in
      *"$VENV_DIR/bin/airflow"*) echo "$pid"; continue ;;
    esac
    [ "$(readlink "$d/cwd" 2>/dev/null)" = "$PROJECT_DIR" ] || continue
    case "$cmd" in
      "airflow "*|"api_server "*|"worker -- "*|"serve-logs"*) echo "$pid" ;;
    esac
  done
}

pids=$(find_pids)
if [ -z "$pids" ]; then
  echo "✓ Airflow của dự án không chạy"
  exit 0
fi
echo "Tiến trình Airflow của dự án:"
for p in $pids; do
  printf '  %-7s %s\n' "$p" "$(tr '\0' ' ' < "/proc/$p/cmdline" 2>/dev/null | cut -c1-100)"
done
[ "$DRY_RUN" = 1 ] && exit 0

# SIGTERM trước: scheduler/worker có cơ hội đánh dấu task đang chạy. Task đang tải/lọc
# dở bị cắt ngang — URL đó chưa ghi DB nên lượt sau làm lại (không hỏng dataset).
kill $pids 2>/dev/null
for _ in $(seq 1 20); do
  sleep 1
  pids=$(find_pids)
  [ -z "$pids" ] && break
done
if [ -n "$pids" ]; then
  echo "⚠  Sau 20s vẫn còn $(echo $pids | wc -w) tiến trình — tắt cưỡng bức (SIGKILL)"
  kill -9 $pids 2>/dev/null
  sleep 1
fi

if ss -ltn 2>/dev/null | grep -qE ':8080\b'; then
  echo "⚠  Cổng 8080 vẫn có tiến trình khác giữ: ss -ltnp | grep 8080"
  exit 1
fi
echo "✓ Đã tắt Airflow"
