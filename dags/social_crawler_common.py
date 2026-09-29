"""
Social CCTV Crawler — logic dùng CHUNG cho các DAG theo platform.

Mỗi platform (YouTube, TikTok, Instagram, X, Reddit, Vimeo, Dailymotion) có
1 DAG RIÊNG — xem social_crawler_<platform>_dag.py — để:
  - Pause/xem lỗi/retry từng platform ĐỘC LẬP trên Airflow UI, không phải
    mở 1 DAG lớn rồi tìm đúng task bị lỗi trong đó.
  - Đặt execution_timeout/retries/schedule RIÊNG cho từng platform (ví dụ
    Reddit chưa cấu hình OAuth cần timeout dài hơn — xem
    social_crawler_reddit_dag.py).
  - 1 platform lỗi liên tục không làm nhiễu lịch sử chạy của platform khác.

File này KHÔNG tự đăng ký DAG nào (không có biến `dag = DAG(...)` ở module
level) nên Airflow bỏ qua khi quét dags_folder — chỉ chứa hàm dùng chung.

── Serialize GPU giữa các DAG ──────────────────────────────────────────────
Bộ phân loại CCTV (CLIP mặc định, hoặc Qwen2.5-VL — CRAWL_CCTV_CLASSIFIER) nạp
model lên GPU; 2 platform chạy CÙNG LÚC có thể OOM (Qwen chắc chắn OOM).
Trước đây (1 DAG duy nhất) dùng max_active_tasks=1 để chạy tuần tự — cách đó
CHỈ có tác dụng trong phạm vi 1 DAG. Giờ mỗi platform là 1 DAG riêng nên
scheduler có thể chạy chúng song song, PHẢI dùng Airflow Pool (GPU_POOL,
1 slot) để giới hạn CHÉO DAG. Tạo pool 1 lần:

    airflow pools set social_crawler_gpu 1 "1 slot — tránh OOM GPU khi \
nhiều platform DAG cùng chạy Qwen2.5-VL"

⚠ Pool nằm trong DB metadata nên `airflow db reset` XÓA MẤT nó. start_airflow.sh
tạo lại pool sau mỗi lần khởi động — đừng bỏ dòng đó, mất pool là mất luôn cơ
chế serialize (task dùng pool không tồn tại sẽ kẹt, không chạy).

── Chạy TUẦN TỰ LIÊN TỤC (rotation) ────────────────────────────────────────
Máy chỉ đủ tài nguyên cho 1 platform tại 1 thời điểm, nhưng ta muốn crawl
liên tục chứ không phải 1 lần/ngày. KHÔNG cần gộp 5 DAG thành 1, cũng không
cần DAG "conductor" gọi TriggerDagRunOperator — chỉ cần 3 thứ đã có sẵn:

    pool 1 slot  → tối đa 1 platform chạy, dù 5 DAG đều muốn chạy
    max_active_runs=1 → mỗi platform không tự chồng lên chính nó
    schedule ngắn (ROTATION_SCHEDULE) → luôn có run xếp hàng chờ slot

Xong 1 platform là slot nhả ra, run đang chờ của platform khác vào ngay →
vòng lặp không nghỉ. Scheduler cấp slot theo (priority_weight desc,
logical_date asc) nên run nào CHỜ LÂU NHẤT được ưu tiên → xoay vòng khá đều.

Muốn thật đều thì đặt thêm trần thời gian mỗi lượt (mặc định 0 = không giới
hạn, khi đó YouTube ~5.6h sẽ chiếm gần hết thời gian máy):

    CRAWL_TASK_TIME_BUDGET_MINUTES=60

Task tự dừng ĐÚNG LỊCH giữa 2 batch (không bị kill giữa lúc đang tải) rồi nhả
slot; lượt sau nhờ dedup L0 nó bỏ qua URL đã xử lý nên tiến độ vẫn tích lũy.

Mọi URL đều đi qua crawler_core.pipeline.VideoPipeline nên dedup 4 tầng là
BẮT BUỘC, không platform nào bỏ qua được:

    L0 URL đã xử lý → L1 canonical ID → download (staging)
    → 9:16 filter → L2 file hash → L3 thumbnail phash → L4 5-frame fingerprint
    → phân loại CCTV trên file đã tải (CLIP + OCR) → chuyển vào dataset

Video bị loại ở các bước sau download được chuyển sang thư mục riêng kèm file
.json ghi lý do (CRAWL_REJECT_ACTION=move, mặc định) hoặc xóa luôn (=delete).
Lý do loại luôn được ghi vào video_urls.reject_reason / filter_detail.

Cấu hình: platform_crawlers/config.py (mọi thứ override được bằng env var).
Tắt bước VLM (không cần GPU, không cần Pool): SOCIAL_USE_CLASSIFIER=false
"""

import os
import sys
import time
import logging
from datetime import datetime as _datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.pool import Pool
from airflow.sdk import Param

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(PROJECT_ROOT, '.env'))
except Exception:
    pass

logger = logging.getLogger(__name__)

CATEGORY       = os.environ.get('SOCIAL_CATEGORY', 'CCTV')
USE_CLASSIFIER = os.environ.get('SOCIAL_USE_CLASSIFIER', 'true').lower() == 'true'

# 1 slot — đảm bảo tối đa 1 platform chạy VLM tại 1 thời điểm, DÙ chúng nằm
# ở các DAG khác nhau. Chỉ áp dụng khi classifier bật (cần GPU); tắt
# classifier thì các DAG platform chạy song song thoải mái (không cần pool).
GPU_POOL = 'social_crawler_gpu'

# Lịch dùng cho MỌI platform DAG để tạo vòng xoay liên tục — xem mục
# "Chạy TUẦN TỰ LIÊN TỤC" ở docstring đầu file. 10 phút chỉ là nhịp ĐẶT LỊCH,
# không phải nhịp chạy thật: pool 1 slot mới quyết định khi nào task được chạy.
# Đặt ở đây (không rải trong 5 file DAG) để đổi nhịp 1 chỗ là xong.
ROTATION_SCHEDULE = timedelta(minutes=10)

# ── Vòng xoay theo THỨ TỰ CỐ ĐỊNH (chain) ────────────────────────────────────
# CRAWL_ROTATION_MODE=chain (mặc định): các DAG platform KHÔNG tự chạy theo lịch.
#   Mỗi DAG chạy xong sẽ trigger DAG kế tiếp theo Variable `crawler_rotation`:
#       {"enabled": true, "order": ["youtube", "dailymotion", "reddit"]}
#   youtube → dailymotion → reddit → youtube → ... Tên trong "order" là tên
#   platform (youtube, dailymotion, reddit, x, facebook).
#     - Bắt đầu vòng: Trigger 1 DAG bất kỳ trong "order", TICK ô "Tiếp tục vòng
#       xoay" (continue_rotation). Không tick = chạy 1 lượt rồi thôi (để thử).
#     - Dừng vòng    : "enabled": false → DAG đang chạy làm xong lượt của nó rồi
#       dừng, không trigger DAG kế. Không kill giữa chừng.
#     - Đổi thứ tự / thêm bớt platform: sửa "order", có hiệu lực từ lần chuyển kế.
#     - Crawl lỗi (hết retry) vẫn chuyển sang DAG kế để vòng không đứt; lượt đó
#       vẫn hiện FAILED trên UI (task crawl_result).
#   ⚠ Pause 1 DAG trong vòng = vòng DỪNG ở đó (lượt được trigger nằm chờ tới khi
#   Unpause) — Airflow 3.1 chưa cho kiểm tra DAG kế có đang pause không. Muốn BỎ
#   QUA platform thì xóa nó khỏi "order".
#   ⚠ Tick "Tiếp tục vòng xoay" khi vòng ĐANG chạy sẽ tạo vòng thứ hai song song.
#   Backfill: DAG vẫn có lịch CHAIN_SCHEDULE (@daily) để giao diện hiện tùy chọn
#   Backfill (Airflow chỉ cho backfill DAG có lịch). Lượt TỰ ĐỘNG theo lịch đó bị
#   bỏ qua ngay (skipped) để không chen vào thứ tự vòng xoay; lượt Backfill và
#   lượt trigger tay crawl bình thường. Backfill N ngày = N lượt crawl liên tiếp
#   của riêng DAG đó (không nối vòng).
# CRAWL_ROTATION_MODE=pool: cơ chế cũ — mọi DAG tự chạy mỗi ROTATION_SCHEDULE,
#   pool 1 slot quyết định ai chạy, thứ tự theo ai chờ lâu nhất (không cố định).
ROTATION_MODE     = os.environ.get('CRAWL_ROTATION_MODE', 'chain').strip().lower() or 'chain'
ROTATION_VARIABLE = 'crawler_rotation'
CHAIN_SCHEDULE    = '@daily'
DEFAULT_ROTATION  = {'enabled': True, 'order': ['youtube', 'dailymotion', 'reddit']}
# dag_id khác mặc định social_crawler_<platform>
_DAG_ID_OVERRIDES = {'facebook': 'social_crawler_fb'}


def platform_dag_id(platform: str) -> str:
    return _DAG_ID_OVERRIDES.get(platform, f'social_crawler_{platform}')

DEFAULT_RETRIES      = 1
DEFAULT_RETRY_DELAY  = timedelta(minutes=15)
DEFAULT_EXEC_TIMEOUT = timedelta(hours=3)
DEFAULT_START_DATE   = _datetime(2026, 7, 30)


# ---------------------------------------------------------------------------
# Bật / tắt bộ lọc theo platform — điều khiển từ giao diện Airflow
# ---------------------------------------------------------------------------
# Ưu tiên (cao → thấp):
#   1. Form Trigger của lượt chạy tay (params filter_*: auto | on | off)
#   2. Airflow Variable `crawler_filters` (Admin → Variables) — áp dụng cho MỌI
#      lượt, kể cả lượt tự động của vòng xoay. Ví dụ tắt lọc CCTV cho YouTube:
#          {"youtube": {"cctv": false}}
#      Khóa "*" áp dụng cho mọi platform: {"*": {"portrait": false}}
#   3. Mặc định: cctv = SOCIAL_USE_CLASSIFIER, portrait = platform không nằm
#      trong CRAWL_ALLOW_PORTRAIT, dedup = bật.
# L0/L1 (trùng URL / ID gốc) KHÔNG tắt được: tắt thì mỗi lượt xử lý lại mọi URL cũ.
FILTER_VARIABLE = 'crawler_filters'
FILTERS = {
    'cctv':     'Lọc CCTV (bộ phân loại CLIP / Qwen)',
    'portrait': 'Loại video dọc 9:16',
    'dedup':    'Loại video trùng nội dung (L2 hash / L3 phash / L4 fingerprint)',
    'videomae': 'Lọc sự việc bằng VideoMAE (có đoạn 5s nhãn khác Normal ≥ 50%)',
}


def resolve_filters(platform: str, params: dict) -> tuple[dict, dict]:
    """Trả ({bộ lọc: bật?}, {bộ lọc: nguồn quyết định}) cho lượt chạy này."""
    from platform_crawlers import config as cfg

    enabled = {
        'cctv':     USE_CLASSIFIER,
        'portrait': platform not in cfg.ALLOW_PORTRAIT,
        'dedup':    True,
        'videomae': os.environ.get('SOCIAL_USE_VIDEOMAE', 'true').lower() == 'true',
    }
    source = dict.fromkeys(enabled, 'mặc định')

    try:
        from airflow.sdk import Variable
        var = Variable.get(FILTER_VARIABLE, default={}, deserialize_json=True) or {}
    except Exception as e:
        logger.warning(f"Không đọc được Variable {FILTER_VARIABLE}: {e} — bỏ qua")
        var = {}
    for scope in ('*', platform):
        for key, val in (var.get(scope) or {}).items():
            if key in enabled and isinstance(val, bool):
                enabled[key] = val
                source[key] = f'Variable {FILTER_VARIABLE}[{scope}]'

    for key in enabled:
        choice = str(params.get(f'filter_{key}') or 'auto').lower()
        if choice in ('on', 'off'):
            enabled[key] = choice == 'on'
            source[key] = 'form Trigger'
    return enabled, source


# ---------------------------------------------------------------------------
# Video bị loại: lưu lại để kiểm tra hay xóa — CHỌN RIÊNG TỪNG LÝ DO LOẠI
# ---------------------------------------------------------------------------
# Người dùng tự xem video trong video_rejected/<lý do>/ để đánh giá từng bộ lọc; bộ lọc
# nào đã tin thì tắt lưu riêng cho nó (Admin → Variables → crawler_reject_action), các
# bộ lọc còn lại vẫn lưu. Áp dụng cho mọi lượt, kể cả lượt tự động của vòng xoay.
#     {"*": "move", "portrait": "delete", "dedup": "delete"}
# Khóa = tên thư mục lý do (portrait, dup_l2, dup_l3, dup_l4, not_cctv, uncertain_cctv,
# no_valid_frame, no_event) hoặc nhóm (dedup, cctv, videomae) hoặc "*". Lý do loại
# LUÔN được ghi vào DB dù move hay delete.
REJECT_VARIABLE = 'crawler_reject_action'


def resolve_reject_actions() -> dict:
    """{lý do loại: 'move'|'delete'} cho lượt này (Variable → env CRAWL_REJECT_ACTION)."""
    from crawler_core.pipeline import REJECT_ACTION, REJECT_GROUPS
    rules = {'*': REJECT_ACTION}
    try:
        from airflow.sdk import Variable
        var = Variable.get(REJECT_VARIABLE, default={}, deserialize_json=True) or {}
    except Exception as e:
        logger.warning(f"Không đọc được Variable {REJECT_VARIABLE}: {e} — dùng {REJECT_ACTION}")
        var = {}
    known = set(REJECT_GROUPS) | set(REJECT_GROUPS.values()) | {'*'}
    for k, v in (var.items() if isinstance(var, dict) else []):
        v = str(v).strip().lower()
        if k in known and v in ('move', 'delete'):
            rules[k] = v
        else:
            logger.warning(f"Variable {REJECT_VARIABLE}: bỏ qua {k!r}={v!r} "
                           f"(khóa hợp lệ: {sorted(known)}; giá trị: move | delete)")
    return rules


def _filter_param(key: str) -> Param:
    return Param(
        'auto', type='string', enum=['auto', 'on', 'off'],
        values_display={'auto': 'Theo cấu hình (Variable / mặc định)',
                        'on': 'Bật', 'off': 'Tắt'},
        title=FILTERS[key],
        description=f'Chỉ áp dụng cho lượt chạy này. Muốn áp dụng lâu dài cho '
                    f'cả lượt tự động: Admin → Variables → {FILTER_VARIABLE}',
    )


def _limit_param(default: int, title: str, desc: str, minimum: int = 1) -> Param:
    return Param(default, type='integer', minimum=minimum, title=title,
                 description=f'{desc} Mặc định {default} (theo cấu hình).')


# Giá trị điền sẵn trên form = đúng giá trị task dùng khi không ai sửa: đọc từ
# platform_crawlers/config.py lúc parse DAG. DAG processor chạy cùng môi trường
# với task (start_airflow.sh export CRAWL_* trước khi khởi động) nên hai bên khớp.
# config.py chỉ đọc env, không import gì nặng — an toàn khi parse DAG mỗi ~30s.
from platform_crawlers import config as _cfg  # noqa: E402

PLATFORM_PARAMS = {
    'continue_rotation': Param(
        False, type='boolean', title='Tiếp tục vòng xoay sau khi chạy xong',
        description=f'Tick = chạy xong sẽ trigger platform kế tiếp theo Variable '
                    f'{ROTATION_VARIABLE} (vòng lặp liên tục). Không tick = chỉ '
                    f'chạy 1 lượt này. Chỉ có tác dụng khi CRAWL_ROTATION_MODE=chain.'),
    'test_mode': Param(
        False, type='boolean', title='Chế độ test (không ghi DB thật)',
        description='Tick = ghi vào bản sao DB + thư mục riêng trong data/.test_runs/, '
                    'không đụng tracker.db, dataset NAS hay trạng thái xoay nhãn. '
                    'Vòng xoay giữ nguyên lựa chọn này cho các DAG kế tiếp.'),
    **{f'filter_{k}': _filter_param(k) for k in FILTERS},
    'time_budget_minutes': _limit_param(
        _cfg.TASK_TIME_BUDGET_MINUTES, 'Trần thời gian lượt chạy (phút)',
        'Task tự dừng ở ranh giới an toàn giữa 2 URL. 0 = không giới hạn.',
        minimum=0),
    'max_per_label': _limit_param(
        _cfg.MAX_PER_LABEL, 'Tối đa URL mỗi nhãn',
        'Số URL tối đa xử lý cho 1 nhãn trong lượt chạy.'),
    'max_per_target': _limit_param(
        _cfg.MAX_PER_TARGET, 'Tối đa URL mỗi từ khóa / nguồn',
        'Số URL lấy về cho mỗi từ khóa — cũng là độ sâu cuộn trang.'),
    'max_per_platform': _limit_param(
        _cfg.MAX_PER_PLATFORM, 'Tối đa URL cả lượt',
        'Trần tổng số URL của cả lượt chạy.'),
}


# ---------------------------------------------------------------------------
# Thân task dùng chung cho MỌI platform
# ---------------------------------------------------------------------------

TEST_RUNS_DIR = os.path.join(PROJECT_ROOT, 'data', '.test_runs')


def _make_sandbox(platform: str) -> dict:
    """
    Chế độ test: thư mục riêng chứa BẢN SAO tracker.db (dedup L0–L4 chạy giống
    thật), thư mục video / video bị loại riêng, bản sao trạng thái xoay nhãn.
    Không ghi gì vào DB thật, dataset NAS hay state/label_cursor.json.
    """
    import shutil
    from crawler_core.db_manager import DB_PATH
    from platform_crawlers import label_cursor

    root = os.path.join(TEST_RUNS_DIR, f"{_datetime.now():%Y%m%d-%H%M%S}_{platform}")
    os.makedirs(root, exist_ok=True)
    db_path = os.path.join(root, 'tracker.db')
    if os.path.exists(DB_PATH):
        shutil.copy2(DB_PATH, db_path)
    cursor = os.path.join(root, 'label_cursor.json')
    if os.path.exists(label_cursor.STATE_PATH):
        shutil.copy2(label_cursor.STATE_PATH, cursor)
    label_cursor.STATE_PATH = cursor       # _load/_save đọc biến module này
    return {
        'root': root, 'db': db_path,
        'video': os.path.join(root, 'video'),
        'rejected': os.path.join(root, 'video_rejected'),
    }


def task_crawl_platform(platform: str, **context):
    """
    Chạy discovery của 1 platform rồi đẩy toàn bộ URL qua VideoPipeline.

    Scraper chỉ tìm URL — không tự tải. Dedup/filter/download/ghi DB đều
    nằm trong pipeline nên không thể bỏ sót.

    Override khi trigger thủ công — form Trigger hiện sẵn các ô (PLATFORM_PARAMS),
    hoặc nhập JSON:
        {"max_per_platform": 50, "max_per_target": 10, "time_budget_minutes": 8,
         "filter_cctv": "off"}
    Bật/tắt bộ lọc: xem resolve_filters().

    time_budget_minutes: task tự dừng ĐÚNG LỊCH sau N phút (kiểm tra giữa
        các nguồn/batch, không kill giữa chừng 1 download/1 lượt discover
        đang chạy) — dùng cho smoke-test xem pipeline có chạy ổn không mà
        không cần đợi hết cả vòng discovery. 0/không đặt = không giới hạn.
    """
    # ── Chế độ chain: lượt tự động theo lịch chỉ để có Backfill → bỏ qua ─────
    dag_run  = context.get('dag_run')
    run_type = getattr(getattr(dag_run, 'run_type', None), 'value',
                       getattr(dag_run, 'run_type', None))
    if ROTATION_MODE == 'chain' and run_type == 'scheduled':
        from airflow.exceptions import AirflowSkipException
        raise AirflowSkipException(
            f'CRAWL_ROTATION_MODE=chain: bỏ qua lượt theo lịch {CHAIN_SCHEDULE} '
            f'(lịch chỉ để bật Backfill). Chạy bằng vòng xoay, trigger tay hoặc Backfill.')

    from crawler_core.db_manager import DBManager
    from crawler_core.downloader import GenericDownloader
    from crawler_core.pipeline import VideoPipeline
    from platform_crawlers import get_scraper
    from platform_crawlers import config as cfg

    # ── Platform có được bật? ────────────────────────────────────────
    if platform not in cfg.ENABLED_PLATFORMS:
        logger.info(
            f"[{platform}] Không nằm trong CRAWL_ENABLED_PLATFORMS "
            f"({'|'.join(cfg.ENABLED_PLATFORMS)}) — bỏ qua"
        )
        return {'platform': platform, 'skipped': 'disabled'}

    # ── Override từ trigger conf ─────────────────────────────────────
    # params = giá trị mặc định của PLATFORM_PARAMS, đã được conf của lượt chạy
    # ghi đè (dag_run_conf_overrides_params=True).
    conf     = context.get('params') or {}
    filters, filter_source = resolve_filters(platform, conf)
    sandbox = _make_sandbox(platform) if conf.get('test_mode') else None
    if sandbox:
        logger.warning(f"[{platform}] CHẾ ĐỘ TEST — mọi thứ ghi vào {sandbox['root']}")
    db_path = sandbox['db'] if sandbox else None
    def _limit(key, default):
        # `is None` chứ không `or`: 0 là giá trị hợp lệ (time_budget 0 = không giới hạn)
        val = conf.get(key)
        return int(default if val is None or val == '' else val)

    max_plat  = _limit('max_per_platform', cfg.MAX_PER_PLATFORM) or cfg.MAX_PER_PLATFORM
    max_targ  = _limit('max_per_target', cfg.MAX_PER_TARGET) or cfg.MAX_PER_TARGET
    max_label = _limit('max_per_label', cfg.MAX_PER_LABEL) or cfg.MAX_PER_LABEL
    time_budget_min = _limit('time_budget_minutes', cfg.TASK_TIME_BUDGET_MINUTES)
    deadline = (
        time.monotonic() + time_budget_min * 60
        if time_budget_min > 0 else None
    )

    # ── Scraper sẵn sàng chưa (cookie, target config...)? ────────────
    # per_label khai báo TRƯỚC scraper vì hai callback dưới đây đóng gói (closure)
    # chính dict này — scraper đọc được tiến độ quota theo thời gian thực.
    #
    # Vì sao cần: trước đây quota nhãn CHỈ được kiểm ở vòng lặp bên dưới, tức là
    # SAU KHI scraper đã cuộn Playwright xong (~1 phút/từ khóa) — kết quả bị ném
    # đi kèm log "Nhãn X đã đủ N/N — bỏ qua". Với Reddit là 17/18 lượt discovery
    # mỗi nhãn chạy không để làm gì. Truyền callback vào để scraper BỎ QUA hẳn
    # lượt discovery đó (xem BaseScraper.iter_label_keywords).
    per_label: dict[str, int] = {}

    def _label_full(label) -> bool:
        return per_label.get(label or CATEGORY, 0) >= max_label

    def _time_up() -> bool:
        return deadline is not None and time.monotonic() >= deadline

    # Số video đã có theo nhãn → scraper sắp nhãn THIẾU NHẤT lên trước
    # (platform_crawlers/label_cursor.order_labels). Đếm 1 lần đầu lượt chạy:
    # thứ tự trong 1 lượt không cần cập nhật realtime, và query này quét cả bảng.
    _db_for_counts = DBManager(db_path)
    label_counts = _db_for_counts.downloaded_by_label()

    scraper = get_scraper(
        platform,
        max_per_target=max_targ,
        label_full=_label_full,
        time_up=_time_up,
        label_counts=label_counts,
    )
    # ── Phiên đăng nhập (platform_crawlers/sessions.py) ───────────────
    # Kiểm tra bằng browser TRƯỚC discovery: cookie X/Facebook có thể chết ở server dù
    # file còn hạn, lúc đó search ra 0 URL mà task vẫn SUCCESS. Chết → tự nhập lại từ
    # Chrome của người dùng; Chrome cũng đăng xuất → báo người dùng (desktop/Telegram)
    # và bỏ qua lượt này (không đụng URL nào), vòng xoay chạy tiếp.
    from platform_crawlers import sessions
    if platform in sessions.PLATFORMS:
        session = sessions.ensure_session(platform)
        (logger.info if session.ok else logger.warning)(
            f"[{platform}] Phiên đăng nhập: {session.state} — {session.message}")
        if session.blocking:
            return {'platform': platform, 'skipped': f'đăng nhập: {session.message}'}

    reason  = scraper.unavailable_reason()
    if reason:
        logger.warning(f"[{platform}] Chưa chạy được: {reason}")
        return {'platform': platform, 'skipped': reason}

    # ── Khởi tạo cổng chung ──────────────────────────────────────────
    db = DBManager(db_path)

    if platform == 'facebook':
        # Facebook KHÔNG tải được bằng yt-dlp thuần: video phát qua DASH,
        # phải bắt từng segment bằng Playwright rồi mux lại (xem
        # crawler_core/facebook_downloader.py). Class này có
        # cùng chữ ký .download(url, category) nên VideoPipeline dùng được
        # y hệt GenericDownloader.
        #
        # Cookie: KHÔNG dùng mặc định cookies/facebook_legacy_cookies.txt —
        # file đó bị qwen_classifier.py dùng làm cookiefile chung cho mọi
        # platform và bị yt-dlp ghi đè (đã thấy thực tế: mất c_user/xs, còn
        # lẫn cookie dailymotion/reddit/x/youtube). Dùng file riêng do
        # scripts/import_browser_cookies.py facebook sinh ra.
        from crawler_core.facebook_downloader import VideoDownloader
        fb_cookies = os.path.join(PROJECT_ROOT, 'cookies', 'facebook_cookies.txt')
        downloader = VideoDownloader(cookies_file=fb_cookies,
                                     **({'output_base': sandbox['video']} if sandbox else {}))
        logger.info(
            f"[{platform}] Dùng VideoDownloader (DASH capture), cookie: "
            f"{fb_cookies if os.path.exists(fb_cookies) else 'THIẾU — sẽ tải hỏng'}"
        )
    else:
        downloader = GenericDownloader(
            platform=platform, max_duration=cfg.max_duration_for(platform),
            **({'output_base': sandbox['video']} if sandbox else {}),
        )

    classifier = None
    if filters['cctv']:
        # CLIP + OCR (mặc định) hoặc Qwen2.5-VL — env CRAWL_CCTV_CLASSIFIER
        from crawler_core.pipeline import load_classifier
        classifier = load_classifier()

    event_filter = None
    if filters['videomae']:
        # Model VideoMAE tự fine-tune — đường dẫn: env VIDEOMAE_* (crawler_core/videomae_filter.py)
        from crawler_core.videomae_filter import VideoMAEEventFilter
        event_filter = VideoMAEEventFilter()

    pipeline = VideoPipeline(
        db=db,
        downloader=downloader,
        classifier=classifier,
        platform=platform,
        category=CATEGORY,
        # TikTok/Reels/Shorts gần như luôn 9:16 — cho phép giữ nếu được cấu hình
        skip_portrait=filters['portrait'],
        dedup_content=filters['dedup'],
        event_filter=event_filter,
        rejected_base=sandbox['rejected'] if sandbox else None,
        reject_action=resolve_reject_actions(),
    )

    from crawler_core.pipeline import REJECT_GROUPS
    from platform_crawlers import labels as L
    from platform_crawlers import label_cursor
    active_labels = L.enabled_labels()
    resume_at = label_cursor.peek(platform)

    logger.info(
        f"\n{'='*60}\n"
        f"PLATFORM: {platform}{'   [CHẾ ĐỘ TEST — ' + sandbox['root'] + ']' if sandbox else ''}\n"
        f"  fingerprint đã có trong DB : {db.fingerprint_count()}\n"
        f"  giới hạn                   : {max_targ}/nguồn, "
        f"{max_label}/nhãn, {max_plat}/platform\n"
        f"  bắt đầu từ nhãn            : "
        f"{resume_at or f'{active_labels[0]} (chưa có con trỏ)'}\n"
        f"  time budget                : "
        f"{f'{time_budget_min} phút' if deadline else 'không giới hạn'}\n"
        f"  phân loại CCTV             : "
        f"{getattr(classifier, 'name', type(classifier).__name__) if classifier else '—'}\n"
        f"  lọc sự việc (VideoMAE)     : {event_filter.name if event_filter else '—'}\n"
        f"  video bị loại → {pipeline.rejected_base}\n"
        + ''.join(
            f"      {o:23}: {'lưu lại để kiểm tra' if pipeline.action_for(o) == 'move' else 'XÓA LUÔN'}\n"
            for o in REJECT_GROUPS)
        + ''.join(
            f"  lọc {k:23}: {'BẬT' if on else 'TẮT'}  ({filter_source[k]})\n"
            for k, on in filters.items())
        + f"  nhãn cần thu ({len(active_labels)})        : {', '.join(active_labels)}\n"
        f"{'='*60}"
    )

    # ── Discovery → pipeline ─────────────────────────────────────────
    # Đếm RIÊNG từng nhãn: 11 nhãn được xử lý lần lượt, nếu chỉ có trần
    # tổng thì nhãn đầu ăn hết quota và nhãn cuối trắng tay.
    processed = 0
    time_up = False

    for urls, source_label, category in scraper.discover():
        # Kiểm tra NGAY khi generator yield — 1 lượt discover() (ytdlp/Playwright)
        # đã trôi qua có thể mất tới vài chục giây/vài phút và không cắt được
        # giữa chừng, nên deadline chỉ chặn được TRƯỚC lượt tiếp theo.
        if deadline is not None and time.monotonic() >= deadline:
            logger.info(
                f"[{platform}] Hết time budget {time_budget_min} phút "
                f"— dừng discovery giữa chừng (đã xử lý {processed} URL)"
            )
            time_up = True
            break

        if processed >= max_plat:
            logger.info(f"[{platform}] Đạt trần tổng {max_plat} URL — dừng")
            break

        key  = category or CATEGORY
        done = per_label.get(key, 0)
        if done >= max_label:
            logger.info(
                f"[{platform}] Nhãn {key} đã đủ {done}/{max_label} — "
                f"bỏ qua {source_label}"
            )
            continue

        if not urls:
            logger.info(f"[{platform}] {source_label}: không tìm được URL nào")
            continue

        # Chỉ nhận phần còn thiếu của nhãn này, không tràn sang nhãn khác
        room = min(max_label - done, max_plat - processed)
        urls = urls[:room]

        db.add_source(source_label, platform, category=category or 'unknown')
        logger.info(
            f"[{platform}] {source_label}: {len(urls)} URL "
            f"→ nhãn {key} ({done}/{max_label})"
        )

        for i in range(0, len(urls), cfg.BATCH_SIZE):
            # Kiểm tra lại TRƯỚC mỗi batch: 1 batch có thể tải nhiều video,
            # tốn vài phút — không muốn đợi hết cả batch mới cắt.
            if deadline is not None and time.monotonic() >= deadline:
                logger.info(
                    f"[{platform}] Hết time budget {time_budget_min} phút "
                    f"— dừng giữa các batch của {source_label} "
                    f"(đã xử lý {processed} URL)"
                )
                time_up = True
                break

            batch = urls[i:i + cfg.BATCH_SIZE]
            # should_stop kiểm TRƯỚC TỪNG URL: trên GPU 6GB, VLM ~48s/URL nên
            # 1 batch 25 URL ≈ 20 phút. Không có nó thì task hết giờ ở phút 60
            # vẫn giữ slot GPU tới phút 80 (đo thực tế 2026-08-03: lần chạy
            # dailymotion đi hết 3h execution_timeout, 9 batch, 225 URL).
            outcome = pipeline.process_batch(
                batch, source_page=source_label, category=category,
                should_stop=_time_up,
            )
            done_in_batch = sum(outcome.values())
            processed += done_in_batch
            per_label[key] = per_label.get(key, 0) + done_in_batch
            logger.info(
                f"[{platform}] {source_label} batch {i // cfg.BATCH_SIZE + 1}: "
                f"{outcome} | lũy kế: {pipeline.stats.summary()}"
            )
            if done_in_batch < len(batch):
                logger.info(
                    f"[{platform}] Hết time budget {time_budget_min} phút — cắt "
                    f"batch giữa chừng ({done_in_batch}/{len(batch)} URL), nhả slot GPU"
                )
                time_up = True
                break

        db.update_source_scraped(source_label, scan_complete=not time_up)

        if time_up:
            break

    stats = pipeline.stats.as_dict()
    by_cat = pipeline.stats.by_category

    # ── Ghi kết quả từng nhãn đã ghé (cho backoff) ───────────────────
    # BẮT BUỘC làm Ở ĐÂY, không thể để scraper tự lo: khi hết giờ, DAG `break`
    # nên generator discover() bị BỎ RƠI — mọi lệnh sau `yield` trong nó không
    # bao giờ chạy. Đây chính là lỗi đã làm con trỏ thế hệ trước đứng yên 5 ngày.
    #
    # per_label = các nhãn ĐÃ GHÉ lượt này; by_cat = nhãn CÓ video lượt này.
    # Nhãn ghé mà không ra video ⇒ tăng chuỗi thất bại; đủ ngưỡng thì
    # label_cursor hạ ưu tiên nó tạm thời, để 1 nhãn vô sản không chiếm slot mãi.
    for label in per_label:
        if label and label != CATEGORY:
            label_cursor.record_visit(platform, label, by_cat.get(label, 0) > 0)

    finish_reason = (
        f'HẾT TIME BUDGET ({time_budget_min} phút)' if time_up
        else 'discovery chạy hết tự nhiên'
    )
    # Không còn "nhãn kế tiếp" nào được lưu: lượt sau TỰ TÍNH LẠI thứ tự từ số
    # video thật trong DB. Chỉ log nhãn vừa làm + nhãn đang bị hạ ưu tiên.
    last_label = label_cursor.peek(platform)
    held = label_cursor.backoff_state(platform)
    visited = [l for l in per_label if l and l != CATEGORY]
    logger.info(
        f"\n{'='*60}\n"
        f"XONG [{platform}] — {finish_reason}\n"
        f"  {pipeline.stats.summary()}\n"
        f"  Video tải được theo nhãn:\n"
        + ('\n'.join(f'    {k:16} {v}' for k, v in sorted(by_cat.items(), key=lambda x: -x[1]))
           or '    (chưa có)')
        + f"\n  Nhãn đã ghé lượt này: {', '.join(visited) or '(không nhãn nào)'}"
        + f"\n  Nhãn cuối đang làm  : {last_label or '—'}"
        + (f"\n  Đang hạ ưu tiên     : "
           + ', '.join(f'{k} ({v}h nữa)' for k, v in sorted(held.items()))
           if held else '')
        + f"\n{'='*60}"
    )
    return {
        'platform': platform,
        'time_budget_hit': time_up,
        'labels_visited': visited,
        'labels_backoff': held,
        'filters': filters,
        'test_sandbox': sandbox['root'] if sandbox else None,
        **stats,
    }


# ---------------------------------------------------------------------------
# Report — tổng hợp mọi platform từ DB (dùng bởi social_crawler_report_dag.py)
# ---------------------------------------------------------------------------

def task_report_stats(**context):
    from crawler_core.db_manager import DBManager

    db    = DBManager()
    stats = db.get_stats()

    logger.info('=' * 60)
    logger.info('TỔNG KẾT CRAWLER (toàn bộ platform)')
    logger.info('=' * 60)
    logger.info(f"URL theo status     : {stats['urls']}")
    logger.info(f"Downloaded theo cat : {stats['categories']}")
    logger.info(f"Fingerprint đã lưu  : {stats['fingerprints']}")
    logger.info('-' * 60)
    logger.info('Video unique theo platform:')
    for platform, count in sorted(
        stats['platforms'].items(), key=lambda x: -x[1]
    ):
        logger.info(f"  {platform:14} {count:>6}")
    logger.info('=' * 60)
    return stats


# ---------------------------------------------------------------------------
# Vòng xoay chain — chọn DAG kế tiếp
# ---------------------------------------------------------------------------

def task_pick_next(platform: str, **context) -> dict:
    """
    Trả {'dag_id': DAG kế tiếp, 'conf': conf cho lượt đó}; skip nếu không tiếp vòng.
    conf = MỌI lựa chọn của lượt hiện tại (trần thời gian, số URL, bộ lọc,
    test_mode...) — vòng xoay giữ nguyên cài đặt của lượt bắt đầu.
    """
    from airflow.exceptions import AirflowSkipException
    from airflow.sdk import Variable

    params = dict(context.get('params') or {})
    if not params.get('continue_rotation'):
        raise AirflowSkipException(
            'Lượt chạy không tick "Tiếp tục vòng xoay" — không trigger DAG kế')

    rot = Variable.get(ROTATION_VARIABLE, default=DEFAULT_ROTATION,
                       deserialize_json=True) or {}
    if not rot.get('enabled', True):
        raise AirflowSkipException(
            f'Vòng xoay đang TẮT ({ROTATION_VARIABLE}.enabled = false) — dừng ở {platform}')

    order = [p.strip().lower() for p in rot.get('order') or [] if str(p).strip()]
    if platform not in order:
        raise AirflowSkipException(
            f'{platform} không nằm trong {ROTATION_VARIABLE}.order {order} — dừng vòng')

    nxt = order[(order.index(platform) + 1) % len(order)]
    logger.info(f'[Vòng xoay] {platform} xong → trigger {nxt} (thứ tự: {" → ".join(order)})')
    return {'dag_id': platform_dag_id(nxt),
            'conf': {**params, 'continue_rotation': True}}


# ---------------------------------------------------------------------------
# Factory — mỗi social_crawler_<platform>_dag.py gọi hàm này ĐÚNG 1 LẦN
# ---------------------------------------------------------------------------

def build_platform_dag(
    platform: str,
    schedule: str | timedelta = ROTATION_SCHEDULE,
    dag_id: str | None = None,
    execution_timeout: timedelta = DEFAULT_EXEC_TIMEOUT,
    retries: int = DEFAULT_RETRIES,
    retry_delay: timedelta = DEFAULT_RETRY_DELAY,
    start_date=None,
    extra_tags: list[str] | None = None,
) -> DAG:
    """
    Tạo 1 DAG hoàn chỉnh, độc lập, cho 1 platform (1 DAG = 1 task).

    Cho phép override execution_timeout/retries/retry_delay RIÊNG cho từng
    platform (ví dụ Reddit cần timeout dài hơn khi chưa có OAuth) mà không
    ảnh hưởng các platform khác — đây chính là lợi ích của việc tách DAG.
    """
    dag_id = dag_id or f'social_crawler_{platform}'
    if ROTATION_MODE == 'chain':
        # Chạy khi được DAG trước trong vòng trigger, trigger tay hoặc Backfill.
        # Lịch @daily chỉ để giao diện hiện Backfill — lượt theo lịch tự skip
        # (xem đầu task_crawl_platform).
        schedule = CHAIN_SCHEDULE

    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': retries,
        'retry_delay': retry_delay,
        'execution_timeout': execution_timeout,
    }

    with DAG(
        dag_id=dag_id,
        description=(
            f'Crawl CCTV video từ {platform} '
            f'— qua cổng dedup 4 tầng dùng chung (crawler_core)'
        ),
        default_args=default_args,
        start_date=start_date or DEFAULT_START_DATE,
        schedule=schedule,
        catchup=False,
        max_active_runs=1,
        params=PLATFORM_PARAMS,
        render_template_as_native_obj=True,
        tags=['social', 'cctv', 'dedup', platform, *(extra_tags or [])],
    ) as dag:
        crawl = PythonOperator(
            task_id=f'crawl_{platform}',
            python_callable=task_crawl_platform,
            op_kwargs={'platform': platform},
            # Giới hạn CHÉO DAG: tối đa 1 platform chạy VLM cùng lúc.
            # Không cần pool khi đã tắt classifier (không đụng GPU).
            pool=GPU_POOL if USE_CLASSIFIER else Pool.DEFAULT_POOL_NAME,
        )

        if ROTATION_MODE == 'chain':
            from airflow.providers.standard.operators.empty import EmptyOperator
            from airflow.providers.standard.operators.trigger_dagrun import (
                TriggerDagRunOperator,
            )
            # all_done: crawl lỗi (hết retry) vẫn chuyển sang platform kế, vòng
            # không bị đứt vì 1 platform hỏng cookie.
            pick = PythonOperator(
                task_id='pick_next_platform',
                python_callable=task_pick_next,
                op_kwargs={'platform': platform},
                trigger_rule='all_done',
            )
            trigger = TriggerDagRunOperator(
                task_id='trigger_next_platform',
                trigger_dag_id="{{ ti.xcom_pull(task_ids='pick_next_platform')['dag_id'] }}",
                # render_template_as_native_obj=True (DAG) → conf là dict thật,
                # không bị render thành chuỗi.
                conf="{{ ti.xcom_pull(task_ids='pick_next_platform')['conf'] }}",
                wait_for_completion=False,
            )
            # Lá riêng nối thẳng sau crawl: crawl lỗi → task này upstream_failed →
            # lượt chạy hiện FAILED trên UI. Thiếu nó thì lá cuối là trigger (thành
            # công) nên Airflow đánh dấu cả lượt SUCCESS, che mất lỗi crawl.
            result = EmptyOperator(task_id='crawl_result')
            crawl >> pick >> trigger
            crawl >> result

    return dag
