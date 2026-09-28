"""
Chọn thứ tự nhãn cho mỗi lượt crawl — ƯU TIÊN NHÃN ĐANG THIẾU VIDEO NHẤT.

LỊCH SỬ 3 THẾ HỆ (đọc để không lặp lại sai lầm cũ)
--------------------------------------------------
**Gen 1 — thứ tự cố định.** `labels.LABELS` là dict có thứ tự và mọi scraper lặp
`for label in by_label`, nên MỌI lượt chạy đều bắt đầu từ `Abuse`. Với trần thời
gian, các nhãn cuối bảng không bao giờ tới lượt.

**Gen 2 — con trỏ xoay vòng (round-robin).** Lưu "nhãn kế tiếp", lượt sau tiếp
tục từ đó. Hỏng vì con trỏ chỉ nhích khi nhãn đạt QUOTA, mà `MAX_PER_LABEL=300`
lớn hơn số URL 1 lượt xử lý nổi (VLM ~48s/URL ⇒ ~75 URL/60 phút) và `per_label`
RESET mỗi lượt ⇒ nhãn không bao giờ đạt quota ⇒ con trỏ đứng yên. Đo 2026-08-05:
reddit và youtube kẹt ở `Abuse` suốt 5 ngày, 4/11 nhãn có 0 video, và 4 nhãn đó
chỉ từng được search 3 lần bởi đúng 1 platform (dailymotion, lúc phiên đang hỏng).

**Gen 3 — hiện tại: chọn theo THIẾU HỤT.** Mỗi lượt tính lại thứ tự từ số video
thật trong DB, nhãn ít nhất đi trước. Không còn "con trỏ" nào để kẹt: hết giờ
giữa nhãn thì lượt sau tính lại và nhãn đó VẪN đang thiếu nên vẫn được ưu tiên —
tự phục hồi, không cần ai nhích gì cả.

Round-robin còn một khuyết điểm nữa mà Gen 3 chữa được: nó chia đều thời gian một
cách mù quáng. Abuse có 990 video, RoadAccident có 0, mà cả hai được cấp số lượt
ghé như nhau ⇒ khoảng cách 990 không bao giờ đóng lại.

BỆNH NGƯỢC VÀ CÁCH CHẶN (backoff)
---------------------------------
Ưu tiên nhãn thiếu nhất có một nguy cơ đối xứng: nhãn nào THẬT SỰ không có nội
dung (platform không có loại video đó, hoặc từ khóa vô vọng) sẽ luôn ở đáy bảng
và chiếm slot VĨNH VIỄN, làm chết 10 nhãn còn lại. Nguy hiểm hơn cả bệnh cũ.

Chặn bằng backoff theo (platform, nhãn) — productivity phụ thuộc platform, ví dụ
FallOver có thể vô vọng trên dailymotion nhưng dồi dào trên youtube:

    ghé N lượt liên tiếp không ra video nào  →  đẩy nhãn xuống CUỐI thứ tự
    thời gian đẩy tăng gấp đôi mỗi lần thất bại thêm, chặn trên 48h
    ra được video  →  reset, nhãn quay lại tranh ưu tiên bình thường

Cố ý ĐẨY XUỐNG CUỐI chứ không bỏ hẳn: nếu còn thời gian trong lượt thì nhãn vẫn
được thử. Không bao giờ mất hẳn một nhãn vì một chuỗi thất bại tạm thời.

STATE
-----
`state/label_cursor.json` — đọc/sửa được bằng mắt:

    {
      "reddit": {
        "current": "RoadAccident",
        "labels": {
          "FallOver": {"empty_streak": 3, "backoff_until": 1785..., "visits": 5,
                       "last_visit": 1785..., "last_video_at": null}
        }
      }
    }

Vẫn đọc được format Gen 2 (`{"reddit": "Abuse"}`) để không mất state khi nâng cấp.

Ghi bằng ghi-tạm-rồi-rename (nguyên tử). Không cần lock: pool `social_crawler_gpu`
chỉ cho 1 task crawler chạy tại 1 thời điểm.

Mọi hàm KHÔNG raise: state hỏng chỉ làm mất tính ưu tiên (quay về thứ tự mặc
định), không được phép làm sập task crawl.
"""

import json
import logging
import os
import time

logger = logging.getLogger(__name__)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_DEFAULT_PATH = os.path.join(REPO_ROOT, 'state', 'label_cursor.json')
STATE_PATH = os.environ.get('CRAWL_LABEL_CURSOR_FILE', _DEFAULT_PATH)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, '') or default)
    except ValueError:
        return default


# Tắt hoàn toàn việc sắp thứ tự (dùng nguyên thứ tự trong labels.py) — chỉ để tái
# hiện lại một lần chạy cũ.
ENABLED = os.environ.get('CRAWL_LABEL_ROTATION', 'true').lower() == 'true'

# Số lượt ghé liên tiếp KHÔNG ra video thì bắt đầu đẩy nhãn xuống cuối.
EMPTY_STREAK_LIMIT = _env_int('CRAWL_LABEL_EMPTY_STREAK', 3)

# Thời gian đẩy xuống cuối lần đầu; gấp đôi mỗi lần thất bại thêm.
BACKOFF_HOURS = _env_int('CRAWL_LABEL_BACKOFF_HOURS', 6)
BACKOFF_MAX_HOURS = _env_int('CRAWL_LABEL_BACKOFF_MAX_HOURS', 48)

# Nhãn đã đạt số video này thì nhường ưu tiên cho nhãn còn thiếu (0 = tắt).
# Chốt an toàn để không đào mãi 1 nhãn khi các nhãn khác còn hụt.
LABEL_TARGET = _env_int('CRAWL_LABEL_TARGET', 0)


# ── Đọc/ghi state ───────────────────────────────────────────────────────────

def _load() -> dict:
    try:
        with open(STATE_PATH) as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception as e:
        logger.warning(f"[LabelCursor] Không đọc được {STATE_PATH}: {e} — coi như rỗng")
        return {}


def _save(data: dict) -> None:
    try:
        os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
        tmp = f'{STATE_PATH}.tmp'
        with open(tmp, 'w') as f:
            json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp, STATE_PATH)          # nguyên tử
    except Exception as e:
        logger.warning(f"[LabelCursor] Không ghi được {STATE_PATH}: {e}")


def _entry(data: dict, platform: str) -> dict:
    """State của 1 platform, tự nâng cấp format Gen 2 (giá trị là chuỗi)."""
    cur = data.get(platform)
    if isinstance(cur, str):                 # Gen 2: {"reddit": "Abuse"}
        cur = {'current': cur, 'labels': {}}
        data[platform] = cur
    elif not isinstance(cur, dict):
        cur = {'current': None, 'labels': {}}
        data[platform] = cur
    cur.setdefault('labels', {})
    return cur


def _usable(label) -> bool:
    """by_label có thể có key None (chế độ adhoc qua CRAWL_*_KEYWORDS override)."""
    return isinstance(label, str) and bool(label)


# ── Đọc trạng thái ──────────────────────────────────────────────────────────

def peek(platform: str) -> str | None:
    """Nhãn platform này đang/vừa làm. Chỉ để log và báo cáo."""
    data = _load()
    cur = data.get(platform)
    if isinstance(cur, str):
        return cur if _usable(cur) else None
    if isinstance(cur, dict):
        v = cur.get('current')
        return v if _usable(v) else None
    return None


def snapshot() -> dict:
    """Toàn bộ state — dùng cho scripts/crawl_report_24h.py."""
    return _load()


def backoff_state(platform: str, now: float | None = None) -> dict:
    """{nhãn: số giờ còn bị đẩy xuống cuối} — chỉ các nhãn ĐANG bị backoff."""
    now = time.time() if now is None else now
    labels = _entry(_load(), platform).get('labels', {})
    out = {}
    for label, st in labels.items():
        until = st.get('backoff_until') or 0
        if until > now:
            out[label] = round((until - now) / 3600, 1)
    return out


# ── Sắp thứ tự nhãn ─────────────────────────────────────────────────────────

def order_labels(
    platform: str,
    labels: list,
    counts: dict | None = None,
    now: float | None = None,
) -> list:
    """
    Thứ tự crawl cho lượt này: nhãn THIẾU VIDEO NHẤT đi trước.

    `counts` = {nhãn: số video đã có} (DBManager.downloaded_by_label()). Không
    truyền → không sắp theo thiếu hụt, chỉ áp dụng backoff (dùng khi test tay).

    Khóa sắp xếp, theo thứ tự ưu tiên:
        1. đang bị backoff / đã đạt LABEL_TARGET  → xuống cuối
        2. số video hiện có                        → ít trước
        3. lâu chưa được ghé                       → cũ trước (phá thế hòa; nếu
           không có, nhiều nhãn cùng 0 video sẽ luôn xếp theo alphabet và nhãn
           đầu bảng bị ghé dồn)
        4. tên nhãn                                → cho kết quả tất định
    """
    if not ENABLED or len(labels) <= 1:
        return list(labels)

    now = time.time() if now is None else now
    counts = counts or {}
    state = _entry(_load(), platform).get('labels', {})

    def key(label):
        st = state.get(label, {}) if _usable(label) else {}
        n = int(counts.get(label, 0) or 0)
        deferred = 0
        if (st.get('backoff_until') or 0) > now:
            deferred = 1
        if LABEL_TARGET and n >= LABEL_TARGET:
            deferred = 1
        return (deferred, n, st.get('last_visit') or 0, str(label))

    ordered = sorted(labels, key=key)

    held = backoff_state(platform, now)
    if held:
        logger.info(
            f"[LabelCursor] {platform}: đẩy xuống cuối vì nhiều lượt không ra "
            f"video — " + ', '.join(f'{k} ({v}h nữa)' for k, v in sorted(held.items()))
        )
    logger.info(
        f"[LabelCursor] {platform}: thứ tự lượt này (nhãn thiếu nhất trước) — "
        + ', '.join(f'{l}:{counts.get(l, 0)}' for l in ordered)
    )
    return ordered


# ── Ghi trạng thái ──────────────────────────────────────────────────────────

def save_current(platform: str, label) -> None:
    """Đánh dấu đang LÀM nhãn này (chỉ để log/báo cáo, không ảnh hưởng thứ tự)."""
    if not _usable(label):
        return
    data = _load()
    entry = _entry(data, platform)
    if entry.get('current') == label:
        return
    entry['current'] = label
    _save(data)


def record_visit(platform: str, label, got_video: bool, now: float | None = None) -> None:
    """
    Ghi kết quả 1 lượt ghé nhãn. GỌI TỪ DAG sau khi lượt chạy kết thúc, KHÔNG
    gọi trong generator discover(): hết giờ thì DAG `break` và generator bị bỏ
    rơi, mọi lệnh sau `yield` trong đó không bao giờ chạy (đúng lỗi đã làm con
    trỏ Gen 2 đứng yên).

    got_video=True  → reset chuỗi thất bại, xóa backoff.
    got_video=False → tăng chuỗi; quá EMPTY_STREAK_LIMIT thì đẩy xuống cuối
                      trong BACKOFF_HOURS × 2^(số lần vượt), chặn trên
                      BACKOFF_MAX_HOURS.
    """
    if not _usable(label):
        return
    now = time.time() if now is None else now
    data = _load()
    entry = _entry(data, platform)
    st = entry['labels'].setdefault(label, {})

    st['visits'] = int(st.get('visits') or 0) + 1
    st['last_visit'] = now

    if got_video:
        if st.get('empty_streak'):
            logger.info(
                f"[LabelCursor] {platform}/{label}: có video → reset chuỗi "
                f"{st['empty_streak']} lượt trắng"
            )
        st['empty_streak'] = 0
        st['backoff_until'] = 0
        st['last_video_at'] = now
    else:
        st['empty_streak'] = int(st.get('empty_streak') or 0) + 1
        over = st['empty_streak'] - EMPTY_STREAK_LIMIT
        if over >= 0:
            hours = min(BACKOFF_HOURS * (2 ** over), BACKOFF_MAX_HOURS)
            st['backoff_until'] = now + hours * 3600
            logger.warning(
                f"[LabelCursor] {platform}/{label}: {st['empty_streak']} lượt liên "
                f"tiếp KHÔNG ra video → hạ ưu tiên {hours}h. Nếu tái diễn nhiều "
                f"lần, platform này có thể không có loại nội dung đó."
            )

    _save(data)


def reset(platform: str | None = None) -> None:
    """Xóa state (1 platform hoặc tất cả) — kể cả backoff."""
    data = {} if platform is None else _load()
    if platform is not None:
        data.pop(platform, None)
    _save(data)
