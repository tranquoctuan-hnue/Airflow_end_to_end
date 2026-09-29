#!/bin/bash
# ═══════════════════════════════════════════════════════════════════════════════
# Cài đặt dự án trên máy MỚI (Ubuntu 22.04, Python 3.10, GPU NVIDIA tùy chọn).
#
#   bash scripts/setup.sh                 # crawler (bắt buộc)
#   bash scripts/setup.sh --rtsp          # + pipeline RTSP (paddle, ultralytics)
#   bash scripts/setup.sh --qwen          # + bộ phân loại Qwen2.5-VL cũ
#   bash scripts/setup.sh --cpu           # máy không có GPU NVIDIA
#
# Chạy lại nhiều lần an toàn: bước nào xong rồi sẽ bỏ qua / cập nhật.
# Gói hệ thống (cần sudo) script KHÔNG tự cài — chỉ báo thiếu gì và lệnh cài.
# ═══════════════════════════════════════════════════════════════════════════════
set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${VENV_DIR:-$PROJECT_DIR/airflow_venv}"
PYTHON="${PYTHON:-python3.10}"
AIRFLOW_VERSION=3.1.6
TORCH_INDEX=https://download.pytorch.org/whl/cu118
WITH_RTSP=0; WITH_QWEN=0

for arg in "$@"; do
  case "$arg" in
    --rtsp) WITH_RTSP=1 ;;
    --qwen) WITH_QWEN=1 ;;
    --cpu)  TORCH_INDEX=https://download.pytorch.org/whl/cpu ;;
    -h|--help) sed -n 2,13p "$0"; exit 0 ;;
    *) echo "Tham số không hợp lệ: $arg (xem --help)"; exit 1 ;;
  esac
done

step() { echo; echo "── $* ──────────────────────────────────────────"; }

# ── 1. Gói hệ thống ──────────────────────────────────────────────────────────
step "1/7 Kiểm tra gói hệ thống"
missing=()
command -v "$PYTHON" >/dev/null || missing+=("python3.10 python3.10-venv")
command -v ffmpeg    >/dev/null || missing+=("ffmpeg")
command -v tesseract >/dev/null || missing+=("tesseract-ocr")
if [ ${#missing[@]} -gt 0 ]; then
  echo "✗ Thiếu: ${missing[*]}"
  echo "  Cài: sudo apt update && sudo apt install -y ${missing[*]}"
  exit 1
fi
echo "✓ $($PYTHON --version), ffmpeg, tesseract"
if ! command -v deno >/dev/null && [ ! -x "$HOME/.deno/bin/deno" ]; then
  echo "⚠  Chưa có deno (yt-dlp cần để tải YouTube). Cài không cần sudo:"
  echo "     curl -fsSL https://deno.land/install.sh | sh"
fi
command -v nvidia-smi >/dev/null && nvidia-smi --query-gpu=name,memory.total --format=csv,noheader \
  || echo "⚠  Không thấy GPU NVIDIA — nên chạy lại với --cpu và tắt bộ lọc GPU trong .env"

# ── 2. Virtualenv ────────────────────────────────────────────────────────────
step "2/7 Virtualenv: $VENV_DIR"
[ -x "$VENV_DIR/bin/python" ] || "$PYTHON" -m venv "$VENV_DIR"
PIP="$VENV_DIR/bin/pip"
"$PIP" install -q --upgrade pip wheel setuptools

# ── 3. Airflow (kèm constraints chính thức — tránh xung đột phiên bản) ───────
step "3/7 Apache Airflow $AIRFLOW_VERSION"
PYVER="$("$VENV_DIR/bin/python" -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")')"
"$PIP" install -q "apache-airflow==$AIRFLOW_VERSION" apache-airflow-providers-standard \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW_VERSION/constraints-$PYVER.txt"

# ── 4. PyTorch + thư viện crawler ────────────────────────────────────────────
step "4/7 PyTorch ($TORCH_INDEX) + requirements.txt"
"$PIP" install -q -r "$PROJECT_DIR/requirements-torch.txt" --index-url "$TORCH_INDEX"
"$PIP" install -q -r "$PROJECT_DIR/requirements.txt"
if [ "$WITH_RTSP$WITH_QWEN" != "00" ]; then
  tmp="$(mktemp)"
  # requirements-optional.txt chia khối (a) RTSP, (b) Qwen — lấy đúng khối cần
  awk -v rtsp=$WITH_RTSP -v qwen=$WITH_QWEN '
    /^# \(a\)/ {blk="a"} /^# \(b\)/ {blk="b"} /^# \(c\)/ {blk="c"}
    /^[a-zA-Z]/ { if ((blk=="a" && rtsp) || (blk=="b" && qwen)) print }' \
    "$PROJECT_DIR/requirements-optional.txt" > "$tmp"
  echo "  + tùy chọn: $(tr '\n' ' ' < "$tmp")"
  # -c requirements.txt: giữ nguyên phiên bản crawler đã chốt. Không có nó, pip nâng numpy
  # 1.26.4 → 2.2.6 để thỏa gói mới → OpenCV 4.6 (build cho numpy 1.x) chết lúc import
  # "numpy.core.multiarray failed to import" — máy 192.169.1.168, 2026-09-29.
  "$PIP" install -q -r "$tmp" -c "$PROJECT_DIR/requirements.txt"; rm -f "$tmp"
  # paddleocr → pdf2docx kéo opencv-python-headless (bản khác) ghi đè CHUNG thư mục cv2/
  # với opencv-python → cv2 hỏng. Dự án không dùng pdf2docx: gỡ headless, cài lại OpenCV.
  if "$PIP" show -q opencv-python-headless >/dev/null 2>&1; then
    "$PIP" uninstall -y -q opencv-python-headless
    "$PIP" install -q --force-reinstall --no-deps \
      "$(grep -E '^opencv-python==' "$PROJECT_DIR/requirements.txt")" \
      "$(grep -E '^opencv-contrib-python==' "$PROJECT_DIR/requirements.txt" || echo opencv-contrib-python==4.6.0.66)"
  fi
  "$VENV_DIR/bin/python" -c "import cv2, numpy" \
    || { echo "✗ cv2/numpy hỏng sau khi cài gói tùy chọn — xem README mục Xử lý sự cố"; exit 1; }
fi

# ── 5. Trình duyệt cho Playwright (discovery X / Reddit / Facebook / Dailymotion)
step "5/7 Playwright Chromium"
"$VENV_DIR/bin/playwright" install chromium
echo "  Nếu Chromium báo thiếu thư viện hệ thống: sudo $VENV_DIR/bin/playwright install-deps chromium"

# ── 6. File cấu hình riêng của máy (không có trong git) ──────────────────────
step "6/7 File cấu hình + thư mục"
cd "$PROJECT_DIR"
[ -f .env ] || { cp .env.example .env; echo "✓ Tạo .env từ mẫu — SỬA CRAWL_VIDEO_OUTPUT_DIR trước khi chạy"; }
[ -f config/gemini_keys.json ] || {
  cp config/gemini_keys.example.json config/gemini_keys.json
  echo "✓ Tạo config/gemini_keys.json — điền Gemini API key"; }
chmod 600 config/gemini_keys.json 2>/dev/null || true
mkdir -p data/db config cookies state reports

# ── 7. Khởi tạo DB metadata Airflow (tự sinh airflow.cfg) + pool GPU ─────────
step "7/7 Khởi tạo Airflow"
export AIRFLOW_HOME="$PROJECT_DIR" PYTHONPATH="$PROJECT_DIR"
"$VENV_DIR/bin/airflow" db migrate >/dev/null
"$VENV_DIR/bin/airflow" pools set social_crawler_gpu 1 "1 slot — tránh OOM GPU" >/dev/null
echo "✓ airflow.cfg + airflow.db đã tạo trong $PROJECT_DIR"

echo
"$VENV_DIR/bin/python" "$PROJECT_DIR/scripts/doctor.py" || true
cat <<EOF

Xong phần cài đặt. Còn lại (xem README.md → "Cài đặt trên máy mới"):
  1. Mở file .env (cấu hình riêng máy này, không vào git) và sửa:
     - CRAWL_VIDEO_OUTPUT_DIR=/thư/mục/lưu/video   ← BẮT BUỘC: mặc định là NAS của máy gốc
     - VIDEOMAE_MODEL_DIR=...   chỉ khi model KHÔNG để ở models/Video_understanding/
     - REDDIT_CLIENT_ID / _SECRET / _USERNAME / _PASSWORD   không bắt buộc: API Reddit
       nhanh hơn trình duyệt; tạo app loại "script" tại https://www.reddit.com/prefs/apps
  2. Chép model vào models/ (models/README.md)
  3. Điền Gemini API key vào config/gemini_keys.json (không có → dùng từ khóa tĩnh)
  4. Mở Chrome, đăng nhập X / Facebook / Reddit / Dailymotion như bình thường, rồi chạy:
     airflow_venv/bin/python scripts/import_browser_cookies.py
     (sau đó crawler tự lấy lại cookie từ Chrome mỗi khi phiên chết)
  5. Chạy: ./start_airflow.sh   → mở http://localhost:8080
EOF
