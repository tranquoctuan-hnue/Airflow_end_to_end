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

# Dừng Airflow CỦA DỰ ÁN NÀY nếu đang chạy (không đụng Airflow/ứng dụng khác trên máy).
# KHÔNG dùng pkill theo đường dẫn venv — sót tiến trình tự đổi tên, xem stop_airflow.sh.
"$PROJECT_DIR/stop_airflow.sh"

# Mọi tiến trình Airflow nhận cwd = thư mục dự án: stop_airflow.sh dựa vào đó để nhận ra
# tiến trình đã tự đổi tên, dù start_airflow.sh được gọi từ thư mục nào.
cd "$PROJECT_DIR"

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
  chmod 600 "$PROJECT_DIR/.env"      # có token Telegram / tài khoản — chỉ chủ máy đọc
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

# ── Variables điều khiển crawler (Admin → Variables) ─────────────────────────
# Mỗi Variable CHỈ tạo khi chưa có — không ghi đè cấu hình người dùng đã sửa. Mô tả
# hiện trong giao diện: bảng chỉ hiện ~50 ký tự đầu (nên dòng đầu là câu tóm tắt),
# rê chuột hiện toàn văn nhưng mất xuống dòng (nên mỗi dòng kết thúc bằng dấu câu),
# form Edit hiện đủ cả xuống dòng. Kiểm tra trên Airflow 3.1.6, 2026-09-29.

# Vòng xoay theo thứ tự cố định (CRAWL_ROTATION_MODE=chain, xem đầu
# dags/social_crawler_common.py).
ROTATION_DESC='Thứ tự chạy các DAG crawler nối tiếp nhau.
"order" = danh sách platform, chạy xong cái này tự chạy cái kế, hết danh sách quay lại đầu. Tên hợp lệ: youtube, dailymotion, reddit, x, facebook.
"enabled": false = dừng vòng: DAG đang chạy làm xong lượt của nó rồi mới dừng.
Bắt đầu vòng: Trigger 1 DAG có trong "order", tick ô "Tiếp tục vòng xoay".
Bỏ qua 1 platform: xóa nó khỏi "order" (đừng Pause DAG, Pause làm cả vòng đứng lại ở đó).
Ví dụ:
{"enabled": true, "order": ["youtube", "dailymotion", "reddit"]} → youtube → dailymotion → reddit → youtube… (mặc định).
{"enabled": true, "order": ["youtube", "dailymotion", "reddit", "x", "facebook"]} → chạy cả 5 platform.
{"enabled": false, "order": ["youtube", "dailymotion", "reddit"]} → dừng vòng sau lượt đang chạy.'
airflow variables get crawler_rotation >/dev/null 2>&1 || \
  airflow variables set crawler_rotation \
    '{"enabled": true, "order": ["youtube", "dailymotion", "reddit"]}' --description "$ROTATION_DESC"

# Bật/tắt bộ lọc theo platform, áp dụng cho cả lượt tự động. Xem resolve_filters()
# trong dags/social_crawler_common.py.
FILTERS_DESC='Bật/tắt bộ lọc cho từng platform.
true = bật, false = tắt. Không ghi = theo .env (mặc định mọi bộ lọc đều bật).
Các bộ lọc: cctv = chỉ giữ video camera CCTV; portrait = loại video dọc; dedup = loại video trùng; videomae = chỉ giữ video có sự việc.
Khóa ngoài cùng: tên platform (youtube, dailymotion, reddit, x, facebook), hoặc "*" = mọi platform. Ghi riêng platform thì thắng "*".
Chọn "on"/"off" trên form Trigger thì thắng Variable này (chỉ cho lượt đó).
Ví dụ:
{} → không đổi gì, theo .env (mặc định).
{"youtube": {"cctv": false}} → YouTube không lọc CCTV, các platform khác vẫn lọc.
{"*": {"videomae": false}} → tắt VideoMAE cho mọi platform.
{"*": {"videomae": false}, "reddit": {"videomae": true}} → tắt VideoMAE mọi nơi, trừ Reddit.'
airflow variables get crawler_filters >/dev/null 2>&1 || \
  airflow variables set crawler_filters '{}' --description "$FILTERS_DESC"

# Lưu video bị loại để kiểm tra hay xóa — chọn riêng từng lý do (tên thư mục trong
# video_rejected/). Xem resolve_reject_actions() trong dags/social_crawler_common.py.
REJECT_DESC='Giữ hay xóa video bị loại, theo từng lý do.
"move" = giữ lại trong video_rejected/<lý do>/ để bạn tự xem bộ lọc có loại đúng không.
"delete" = xóa file luôn (DB vẫn nhớ URL nên không tải lại).
Các lý do: portrait = video dọc; dup_l2, dup_l3, dup_l4 = trùng video đã có; not_cctv, uncertain_cctv, no_valid_frame = không phải (hoặc không chắc là) camera CCTV; no_event = VideoMAE không thấy sự việc.
Viết gọn cả nhóm bằng: dedup, cctv, videomae. "*" = mọi lý do chưa ghi.
Ví dụ:
{"*": "move"} → giữ hết để kiểm tra (mặc định).
{"*": "move", "portrait": "delete", "dedup": "delete"} → xóa video dọc và video trùng, còn lại giữ.
{"*": "delete", "videomae": "move"} → chỉ giữ video bị VideoMAE loại, còn lại xóa.
{"*": "delete"} → xóa hết (ổ đầy, hoặc đã tin mọi bộ lọc).'
airflow variables get crawler_reject_action >/dev/null 2>&1 || \
  airflow variables set crawler_reject_action '{"*": "move"}' --description "$REJECT_DESC"

# `variables set` ghi đè cả giá trị → máy đã có Variable từ trước sẽ giữ mô tả cũ mãi.
# Cập nhật RIÊNG cột description, không đụng giá trị người dùng đã sửa.
export ROTATION_DESC FILTERS_DESC REJECT_DESC
python - <<'PY'
import os
from airflow.models import Variable
from airflow.utils.session import create_session
DESC = {'crawler_rotation': 'ROTATION_DESC', 'crawler_filters': 'FILTERS_DESC',
        'crawler_reject_action': 'REJECT_DESC'}
with create_session() as s:
    for key, env in DESC.items():
        v = s.query(Variable).filter(Variable.key == key).one_or_none()
        if v is not None and v.description != os.environ[env]:
            v.description = os.environ[env]
            print(f'✓ Cập nhật mô tả Variable {key}')
PY

# File mật khẩu web: chỉ chủ máy đọc được (Airflow tạo ra với quyền 664 — mọi tài khoản
# Linux trên máy đều đọc được mật khẩu admin).
PW_FILE="$PROJECT_DIR/simple_auth_manager_passwords.json.generated"
[ -f "$PW_FILE" ] || { touch "$PW_FILE"; }
chmod 600 "$PW_FILE"

# Chạy qua SSH / dịch vụ nền thì thiếu biến của phiên desktop → notify-send và GNOME
# Keyring (giải mã cookie Chrome, platform_crawlers/browser_cookies.py) không tới được
# phiên đang mở. Nối vào bus của phiên desktop nếu nó còn đó.
if [ -z "$DBUS_SESSION_BUS_ADDRESS" ] && [ -S "/run/user/$(id -u)/bus" ]; then
  export DBUS_SESSION_BUS_ADDRESS="unix:path=/run/user/$(id -u)/bus"
  export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"
fi

# Start Airflow standalone
echo "Starting Airflow Standalone at $AIRFLOW_HOME"
echo "Web UI: http://localhost:8080 — tài khoản: $AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS"
echo "  Mật khẩu: cat $PROJECT_DIR/simple_auth_manager_passwords.json.generated"
echo "DAGs folder: $PROJECT_DIR/dags"
airflow standalone
