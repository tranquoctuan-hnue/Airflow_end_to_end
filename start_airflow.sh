#!/bin/bash
# Khởi động toàn bộ Airflow (scheduler, dag-processor, triggerer, web :8080) cho dự án.
# Chạy: ./start_airflow.sh      — lần đầu trên máy mới: chạy scripts/setup.sh trước.
set -e

# Thư mục dự án = nơi chứa script này (clone về đâu cũng chạy được)
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${VENV_DIR:-$PROJECT_DIR/airflow_venv}"

if [ ! -x "$VENV_DIR/bin/airflow" ]; then
  echo "✗ Không thấy Airflow trong $VENV_DIR — chạy trước: bash scripts/setup.sh"
  exit 1
fi

# Dừng Airflow CỦA DỰ ÁN NÀY nếu đang chạy (chỉ tiến trình dùng venv của dự án,
# không đụng Airflow/ứng dụng khác trên máy).
pkill -f "$VENV_DIR/bin/airflow" 2>/dev/null || true
sleep 2

# AIRFLOW_HOME = thư mục dự án: airflow.cfg, airflow.db, logs/ nằm ở đây
# (airflow.cfg / airflow.db tự sinh lần đầu, không đưa lên git).
export AIRFLOW_HOME="$PROJECT_DIR"
export PYTHONPATH="$PROJECT_DIR${PYTHONPATH:+:$PYTHONPATH}"

# deno = JS runtime yt-dlp cần để giải mã YouTube. Cài bằng scripts/setup.sh.
[ -f "$HOME/.deno/env" ] && . "$HOME/.deno/env"
[ -f "$HOME/apps/deno/env" ] && . "$HOME/apps/deno/env"
command -v deno >/dev/null || echo "⚠  Không thấy deno — YouTube có thể tải lỗi (xem README)"

# Activate virtual environment
source "$VENV_DIR/bin/activate"

# ── Cấu hình crawler tuần tự liên tục ──────────────────────────────────────
# Trần thời gian MỖI LƯỢT của 1 platform. 5 DAG social_crawler_* dùng chung pool
# 1 slot nên chúng xoay vòng; không có trần này thì YouTube (~5.6h/lượt) sẽ giữ
# slot hàng giờ và 4 platform còn lại gần như không được chạy. Task tự dừng đúng
# lịch GIỮA 2 BATCH rồi nhả slot, lượt sau dedup L0 bỏ qua URL đã xử lý nên
# tiến độ vẫn tích lũy. Đổi số này để xoay nhanh hơn (30) hay sâu hơn (120).
export CRAWL_TASK_TIME_BUDGET_MINUTES=60

# Bật VLM. ⚠ Đặt =false sẽ ÂM THẦM chuyển các task crawler về default_pool
# (xem build_platform_dag) → mất luôn cơ chế chỉ-1-platform-tại-1-thời-điểm.
export SOCIAL_USE_CLASSIFIER=true

# Chọn nhãn theo THIẾU HỤT: mỗi lượt tính lại thứ tự từ số video thật trong DB,
# nhãn ít video nhất đi trước. Không có nó thì thứ tự nhãn cố định và các nhãn
# cuối bảng không bao giờ tới lượt khi có trần thời gian.
# Xem platform_crawlers/label_cursor.py; state ở state/label_cursor.json.
export CRAWL_LABEL_ROTATION=true

# Chốt an toàn cho cơ chế trên: nhãn ghé N lượt liên tiếp KHÔNG ra video nào thì
# bị đẩy xuống cuối thứ tự trong một khoảng thời gian (gấp đôi mỗi lần thất bại
# thêm). Không có nó, một nhãn thật sự không có nội dung sẽ luôn ở đáy bảng
# "thiếu nhất" và chiếm slot VĨNH VIỄN, làm chết 10 nhãn còn lại.
export CRAWL_LABEL_EMPTY_STREAK=3
export CRAWL_LABEL_BACKOFF_HOURS=6

# Nhãn đạt đủ số video này thì nhường ưu tiên cho nhãn còn thiếu (0 = tắt).
# Bật khi đã có dataset tương đối đều và muốn chặn trần cứng mỗi nhãn.
export CRAWL_LABEL_TARGET=0

# Độ sâu cuộn trang. MAX_PER_TARGET là nút chính: vòng cuộn dừng ở
# `len(found) >= limit` nên tăng MAX_SCROLLS một mình KHÔNG có tác dụng.
# Quá hạn discovery không còn mất kết quả (trả về phần đã gom được).
export CRAWL_MAX_PER_TARGET=100
export CRAWL_MAX_SCROLLS=60
export CRAWL_DISCOVERY_TIMEOUT=600

# Video bị bộ lọc loại (9:16, trùng L2–L4, VLM không phải CCTV): mọi video đều được
# TẢI VỀ TRƯỚC rồi mới lọc. move = chuyển sang thư mục riêng kèm <file>.json ghi lý
# do để tự kiểm tra bộ lọc có loại đúng không; delete = xóa luôn khi đã tin bộ lọc.
# Mặc định chỗ chứa: tuantq/video_rejected/<lý do>/<nhãn>/ (cạnh thư mục dataset),
# đổi bằng CRAWL_REJECTED_DIR. Lý do loại luôn được ghi vào DB dù move hay delete.
export CRAWL_REJECT_ACTION=move

# Bộ phân loại CCTV chạy trên file đã tải:
#   clip = crawler_core/cctv_detector.py — tách cảnh, CLIP + OCR timestamp + fps thực
#   qwen = Qwen2.5-VL cũ trên 1 frame (để so sánh / quay lại)
# Probe tự huấn luyện (python crawler_core/cctv_detector.py train ...) đặt tại
# models/cctv_probe.joblib sẽ được dùng tự động, chính xác hơn zero-shot nhiều.
export CRAWL_CCTV_CLASSIFIER=clip

# chain = chạy lần lượt theo Variable crawler_rotation (youtube → dailymotion →
#         reddit → youtube...), mỗi DAG xong tự trigger DAG kế.
# pool  = cơ chế cũ: mọi DAG chạy theo lịch 10 phút, pool 1 slot chọn ai chạy.
export CRAWL_ROTATION_MODE=chain

# Bộ lọc cuối cùng: model VideoMAE tự fine-tune (ViT-L, 20 lớp). Video ĐẠT khi có ít nhất 1
# đoạn 5s mang nhãn khác Normal với xác suất >= VIDEOMAE_THRESHOLD. Tắt theo platform:
# filter_videomae trên form Trigger hoặc Variable crawler_filters {"x": {"videomae": false}}.
# VIDEOMAE_AGGREGATE=max: đoạn đạt nếu BẤT KỲ clip ~2s nào trong đoạn đạt ngưỡng (bắt
# được sự việc ngắn); =mean: trung bình 5×3 view như segment_infer.py (bỏ sót nhiều hơn).
# Đường dẫn model/nhãn/repo: xem đầu crawler_core/videomae_filter.py.
export SOCIAL_USE_VIDEOMAE=true
export VIDEOMAE_THRESHOLD=0.5
export VIDEOMAE_AGGREGATE=max
export VIDEOMAE_MODEL_DIR="$PROJECT_DIR/models/Video_understanding/vitl_224x224_240926"

# ── Tài khoản web (SimpleAuthManager) — dạng tên:vai_trò, cách nhau bằng dấu phẩy ──
# Mật khẩu tự sinh lần đầu vào simple_auth_manager_passwords.json.generated (không vào git).
# Quyền (đọc từ simple_auth_manager.py của Airflow 3.1.6):
#   viewer  chỉ xem DAG, lượt chạy, log
#   user    + Trigger (kể cả bật/tắt bộ lọc trên form), tạm dừng DAG, chạy lại task.
#           KHÔNG xem/sửa Connections (mật khẩu camera), Variables, Pools, KHÔNG Backfill
#   op      + Connections, Variables (crawler_rotation, crawler_filters), Pools, Backfill
#   admin   toàn quyền
# Thêm người: nối thêm "ten:vai_tro" (ghi đè được trong .env). Xóa người: bỏ khỏi danh
# sách — Airflow tự xóa mật khẩu của họ khỏi file. Cấp lại mật khẩu 1 người: xóa mục của
# người đó trong file, lần khởi động sau Airflow sinh mật khẩu mới 16 ký tự.
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS="${AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS:-admin:admin,op:op,user:user}"

# ── Cấu hình riêng của MÁY NÀY: .env (không đưa lên git, mẫu ở .env.example) ──
# Nạp SAU các giá trị mặc định ở trên để .env ghi đè được chúng (vd. đường dẫn
# lưu video, model, NAS, tài khoản Reddit/Facebook).
if [ -f "$PROJECT_DIR/.env" ]; then
  set -a; . "$PROJECT_DIR/.env"; set +a
else
  echo "⚠  Chưa có .env — dùng mặc định. Tạo từ mẫu: cp .env.example .env"
fi

# ── Kiểm tra NAS đã mount chưa ───────────────────────────────────────────────
# Video được lưu lên NAS pvn_share qua GVFS/SMB. GVFS mount theo PHIÊN ĐĂNG NHẬP
# desktop nên đăng xuất/khởi động lại là mất — lúc đó mọi DAG crawler sẽ chết ở
# bước tạo thư mục lưu. Cảnh báo ngay từ đầu thay vì để phát hiện sau vài giờ.
VIDEO_DIR="${CRAWL_VIDEO_OUTPUT_DIR:-${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/gvfs/smb-share:server=pvn_nas.local,share=pvn_share/tuantq/video}"
if [ -d "$VIDEO_DIR" ]; then
  echo "NAS OK — video sẽ lưu vào: $VIDEO_DIR"
else
  echo "⚠  CHƯA MOUNT NAS: $VIDEO_DIR"
  echo "   Mount: gio mount smb://pvn_nas.local/pvn_share"
  echo "   (hoặc Files → Other Locations → smb://pvn_nas.local/pvn_share)"
  echo "   Hoặc lưu tạm vào ổ nội bộ:"
  echo "     export CRAWL_VIDEO_OUTPUT_DIR=$PROJECT_DIR/data/videos"
  echo "   Các DAG crawler sẽ LỖI cho tới khi xử lý xong."
fi
[ -d "$VIDEOMAE_MODEL_DIR" ] || echo "⚠  Không thấy model VideoMAE: $VIDEOMAE_MODEL_DIR (xem models/README.md)"

# Initialize database schema (migrate, KHÔNG reset)
# ⚠ Dòng này TRƯỚC ĐÂY là `airflow db reset --yes` — nó xóa trắng DB metadata
# mỗi lần khởi động: mất toàn bộ lịch sử chạy, connections, VÀ pool
# social_crawler_gpu. Mất pool nghĩa là task khai báo pool không tồn tại sẽ kẹt
# không chạy → sập cả cơ chế serialize GPU. Muốn reset thật thì chạy tay.
echo "Migrating Airflow database..."
airflow db migrate

# Pool 1 slot — giới hạn CHÉO DAG: tối đa 1 task nạp Qwen2.5-VL tại 1 thời điểm.
# Idempotent (set = tạo mới hoặc cập nhật) nên chạy lại mỗi lần khởi động là an
# toàn, và bù lại được nếu DB từng bị reset.
echo "Ensuring GPU pool exists..."
airflow pools set social_crawler_gpu 1 \
  "1 slot — tránh OOM GPU khi nhiều platform DAG cùng chạy Qwen2.5-VL"

# Bảng bật/tắt bộ lọc theo platform (Admin → Variables → crawler_filters), áp dụng
# cho cả lượt tự động. Ví dụ tắt lọc CCTV cho YouTube: {"youtube": {"cctv": false}}
# Khóa: cctv | portrait | dedup; "*" = mọi platform. Xem resolve_filters() trong
# dags/social_crawler_common.py. CHỈ tạo khi chưa có — không ghi đè cấu hình đã sửa.
# Vòng xoay theo thứ tự cố định (CRAWL_ROTATION_MODE=chain, xem đầu
# dags/social_crawler_common.py). Sửa "order" để đổi thứ tự / thêm bớt platform,
# "enabled": false để dừng vòng sau lượt hiện tại. CHỈ tạo khi chưa có.
airflow variables get crawler_rotation >/dev/null 2>&1 || \
  airflow variables set crawler_rotation \
    '{"enabled": true, "order": ["youtube", "dailymotion", "reddit"]}' \
    --description 'Vòng xoay DAG crawler: order = thứ tự platform, enabled=false để dừng sau lượt hiện tại'

airflow variables get crawler_filters >/dev/null 2>&1 || \
  airflow variables set crawler_filters '{}' \
    --description 'Bật/tắt bộ lọc theo platform. VD: {"youtube": {"cctv": false}}. Khóa: cctv | portrait | dedup; "*" = mọi platform'

# File mật khẩu web: chỉ chủ máy đọc được (Airflow tạo ra với quyền 664 — mọi tài khoản
# Linux trên máy đều đọc được mật khẩu admin).
PW_FILE="$PROJECT_DIR/simple_auth_manager_passwords.json.generated"
[ -f "$PW_FILE" ] || { touch "$PW_FILE"; }
chmod 600 "$PW_FILE"

# Start Airflow standalone
echo "Starting Airflow Standalone at $AIRFLOW_HOME"
echo "Web UI: http://localhost:8080 — tài khoản: $AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS"
echo "  Mật khẩu: cat $PROJECT_DIR/simple_auth_manager_passwords.json.generated"
echo "DAGs folder: $PROJECT_DIR/dags"
airflow standalone
