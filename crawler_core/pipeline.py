"""
VideoPipeline — CỔNG BẮT BUỘC mọi video từ mọi platform phải đi qua.

Không platform nào được tự gọi downloader trực tiếp. Scraper chỉ có một
nhiệm vụ: tìm ra URL. Toàn bộ dedup / filter / download / ghi DB nằm ở đây,
nên thêm platform mới KHÔNG THỂ bỏ sót bước dedup.

Thứ tự xử lý — TẢI VỀ TRƯỚC, LỌC SAU (dừng ngay khi bị loại, rẻ trước đắt sau):

    URL
     │
     ├─ L0  URL đã xử lý xong trong DB?          → skip (0 chi phí, không tải)
     ├─ L1  canonical ID đã có?                  → skip (1 SQL, không tải)
     │       (resolve short link trước nếu cần)
     ├─ ⬇  DOWNLOAD vào ổ nội bộ (staging)
     ├─ 9:16 portrait filter (tùy chọn)          ┐
     ├─ L2  SHA-256 file hash                    │ bị loại → file sang
     ├─ L3  phash 1 frame (50%)                  │ REJECTED_BASE/<lý do>/<nhãn>/
     ├─ L4  phash 5 frame (10/30/50/70/90%)      │ + <file>.json ghi lý do
     ├─ Phân loại CCTV trên TOÀN BỘ FILE         │ (hoặc xóa, xem REJECT_ACTION)
     ├─ VideoMAE: có đoạn 5s nhãn ≠ Normal ≥50%?  ┘ (crawler_core/videomae_filter.py)
     └─ ✓  chuyển file vào <OUTPUT_BASE>/<nhãn>/ + lưu fingerprint 4 tầng

Vì sao tải trước rồi mới lọc: để MỌI video bị loại đều còn file mà xem lại, tự
đánh giá bộ lọc có loại đúng không. Bộ phân loại CCTV chạy SAU dedup vì đắt
nhất (giải mã cả video + CLIP + OCR) — video trùng không cần tốn công đó.

Bộ phân loại (env CRAWL_CCTV_CLASSIFIER, xem load_classifier()):
    clip (mặc định) crawler_core/cctv_detector.py — tách cảnh, chấm từng cảnh
                    bằng CLIP + OCR timestamp + fps thực + đen trắng. Video được
                    nhận khi tổng thời lượng đoạn CCTV >= ngưỡng. Kết quả
                    'uncertain' bị loại riêng vào uncertain_cctv/ để xem lại.
    qwen            Qwen2.5-VL cũ trên 1 frame giữa video (để so sánh/quay lại).
Ảnh đoạn có điểm cao nhất (hoặc frame Qwen đã xem) được lưu kèm video bị loại.

L0/L1 vẫn chạy TRƯỚC khi tải: đó là trùng URL / trùng ID gốc của platform, tức
chắc chắn cùng 1 video — không có gì để đánh giá, tải lại chỉ tốn băng thông.

L1 chỉ bắt trùng trong cùng platform. L2/L3/L4 không filter platform →
chính chúng bắt cùng một clip CCTV được đăng lại trên FB + TikTok + YouTube.

── Video bị loại: di chuyển hay xóa — CHỌN RIÊNG TỪNG LÝ DO LOẠI ─────────────
    move    chuyển sang REJECTED_BASE/<lý do>/<nhãn>/ + <file>.json để người kiểm tra
    delete  xóa luôn — khi đã tin bộ lọc đó
  Mỗi lý do (= tên thư mục con của video_rejected/) chọn riêng qua Variable
  `crawler_reject_action` trên web, ví dụ đã tin lọc 9:16 và trùng lặp:
      {"*": "move", "portrait": "delete", "dedup": "delete"}
  Khóa: portrait | dup_l2 | dup_l3 | dup_l4 | not_cctv | uncertain_cctv |
        no_valid_frame | no_event, nhóm "dedup" = dup_l2/l3/l4, "cctv" = not_cctv/
        uncertain_cctv/no_valid_frame, "*" = còn lại. Ưu tiên: lý do > nhóm > "*" >
        env CRAWL_REJECT_ACTION (mặc định move). Xem resolve_reject_actions() trong
        dags/social_crawler_common.py.
    CRAWL_REJECTED_DIR=...      đổi chỗ chứa (mặc định cạnh OUTPUT_BASE, xem
                                crawler_core/downloader.py REJECTED_BASE)
Dù move hay delete, lý do loại LUÔN được ghi vào video_urls.reject_reason và
video_urls.filter_detail (JSON) — xóa file không làm mất dữ liệu đánh giá.
"""

import json
import logging
import os
from datetime import datetime

from crawler_core import dedup
from crawler_core.downloader import REJECTED_BASE, move_file, release_staged

logger = logging.getLogger(__name__)


def load_classifier(kind: str | None = None):
    """
    Nạp bộ phân loại CCTV theo env CRAWL_CCTV_CLASSIFIER:
        clip (mặc định)  crawler_core.cctv_detector.CCTVVideoClassifier
        qwen             crawler_core.qwen_classifier.CCTVClassifier
    Import trễ để DAG parse nhanh và không nạp model lúc Airflow quét file.
    """
    kind = (kind or os.environ.get('CRAWL_CCTV_CLASSIFIER', 'clip')).strip().lower()
    if kind == 'clip':
        from crawler_core.cctv_detector import CCTVVideoClassifier
        return CCTVVideoClassifier()
    if kind == 'qwen':
        from crawler_core.qwen_classifier import CCTVClassifier
        return CCTVClassifier()
    raise ValueError(
        f"CRAWL_CCTV_CLASSIFIER={kind!r} không hợp lệ — chỉ nhận 'clip' hoặc 'qwen'"
    )

REJECT_ACTION = os.environ.get('CRAWL_REJECT_ACTION', 'move').strip().lower() or 'move'

# Lý do loại → nhóm (khớp FILTERS trong dags/social_crawler_common.py) — để bật/tắt lưu
# theo cả nhóm ("dedup": "delete") hoặc từng lý do ("dup_l3": "move").
REJECT_GROUPS = {
    'portrait': 'portrait',
    'dup_l2': 'dedup', 'dup_l3': 'dedup', 'dup_l4': 'dedup',
    'not_cctv': 'cctv', 'uncertain_cctv': 'cctv', 'no_valid_frame': 'cctv',
    'no_event': 'videomae',
}


def action_for(outcome: str, rules, default: str = REJECT_ACTION) -> str:
    """move/delete cho 1 lý do loại. rules: str (chung mọi lý do) hoặc dict
    {lý do | nhóm | '*': 'move'|'delete'}; ưu tiên lý do > nhóm > '*' > default."""
    if isinstance(rules, str):
        return rules
    rules = rules or {}
    for key in (outcome, REJECT_GROUPS.get(outcome), '*'):
        if key and key in rules:
            return rules[key]
    return default

# Mô tả lý do loại bằng lời — ghi vào file .json cạnh video bị loại
_REASON_TEXT = {
    'portrait': 'Video dọc tỉ lệ 9:16',
    'dup_l1':   'Trùng ID gốc của platform với video đã có (L1)',
    'dup_l2':   'Trùng SHA-256 với video đã có — cùng 1 file (L2)',
    'dup_l3':   'Trùng phash frame giữa với video đã có (L3)',
    'dup_l4':   'Trùng fingerprint 5 frame với video đã có (L4)',
    'not_cctv': 'Bộ phân loại đánh giá không phải footage camera CCTV',
    'uncertain_cctv': 'Có đoạn nghi CCTV nhưng chưa đủ ngưỡng để kết luận — cần xem lại',
    'no_valid_frame': 'Không giải mã được video / không có frame hợp lệ để phân loại',
    'no_event': 'VideoMAE: không có đoạn 5s nào mang nhãn khác Normal với xác suất >= ngưỡng',
}

# Vị trí trích frame cho VLM (tỉ lệ thời lượng). Thử lần lượt tới khi được 1
# frame hợp lệ — frame giữa đôi khi đen (chuyển cảnh) hoặc lỗi decode.
_VLM_FRAME_POSITIONS = (0.5, 0.3, 0.7)


# ── Kết quả xử lý 1 URL ──────────────────────────────────────────────────────
DOWNLOADED   = 'downloaded'
DUP_URL      = 'dup_url'          # L0 — URL này đã xử lý xong trước đó
DUP_L1       = 'dup_l1'           # canonical ID trùng
DUP_L2       = 'dup_l2'           # file hash trùng
DUP_L3       = 'dup_l3'           # thumbnail phash trùng
DUP_L4       = 'dup_l4'           # 5-frame fingerprint trùng
NOT_CCTV     = 'not_cctv'
PORTRAIT     = 'portrait'
NO_FRAME     = 'no_valid_frame'   # không giải mã được / mọi frame đen-hỏng
UNCERTAIN    = 'uncertain_cctv'   # bộ phân loại CLIP trả 'uncertain'
NO_EVENT     = 'no_event'         # VideoMAE: không đoạn nào có nhãn khác Normal >= ngưỡng
FAILED       = 'failed'
ERROR        = 'error'

_DUP_OUTCOMES = (DUP_URL, DUP_L1, DUP_L2, DUP_L3, DUP_L4)

# Status ghi vào video_urls.status
_STATUS_MAP = {
    DOWNLOADED: 'downloaded',
    DUP_L1:     'skipped_duplicate',
    DUP_L2:     'skipped_duplicate',
    DUP_L3:     'skipped_duplicate',
    DUP_L4:     'skipped_duplicate',
    NOT_CCTV:   'skipped_not_cctv',
    PORTRAIT:   'skipped_not_cctv',
    NO_FRAME:   'skipped_not_cctv',
    UNCERTAIN:  'skipped_not_cctv',
    NO_EVENT:   'skipped_no_event',
    FAILED:     'failed',
    ERROR:      'failed',
}

# URL có status này coi như đã xử lý xong — không làm lại (L0)
_TERMINAL_STATUSES = frozenset({
    'downloaded', 'skipped_duplicate', 'skipped_not_cctv', 'skipped_no_event',
})


def _label_tokens(name: str) -> list[str]:
    """'GunRobber' / 'Gun-toting robber' / 'Motor_theft' → từ viết thường."""
    import re
    spaced = re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', name or '')
    return [t for t in re.split(r'[^0-9a-zA-Z]+', spaced.lower()) if t]


def _same_label(a: str, b: str) -> bool:
    """So nhãn giữa 2 hệ đặt tên (crawler ↔ model VideoMAE). Khớp khi: giống nhau sau
    khi bỏ ký tự đặc biệt (Road Accident ~ RoadAccident), tập từ của bên này nằm trọn
    trong bên kia (GunRobber ~ Gun-toting robber), hoặc tiền tố >= 4 ký tự (Smok ~ Smoke)."""
    ta, tb = _label_tokens(a), _label_tokens(b)
    na, nb = ''.join(ta), ''.join(tb)
    if not na or not nb:
        return False
    if na == nb or set(ta) <= set(tb) or set(tb) <= set(ta):
        return True
    short, long_ = sorted((na, nb), key=len)
    return len(short) >= 4 and long_.startswith(short)


class PipelineStats:
    """Bộ đếm kết quả, dùng để log và trả về từ Airflow task."""

    _KEYS = (
        'seen', 'new_urls', 'cctv', DOWNLOADED,
        DUP_URL, DUP_L1, DUP_L2, DUP_L3, DUP_L4,
        NOT_CCTV, UNCERTAIN, NO_EVENT, PORTRAIT, NO_FRAME, FAILED, ERROR,
    )

    def __init__(self):
        self._c = {k: 0 for k in self._KEYS}
        # Đếm video tải được theo từng nhãn sự việc (Arson, Robbery, ...)
        self._by_category: dict[str, int] = {}

    def bump(self, key: str, n: int = 1):
        self._c[key] = self._c.get(key, 0) + n

    def bump_category(self, category: str, n: int = 1):
        self._by_category[category] = self._by_category.get(category, 0) + n

    def __getitem__(self, key):
        return self._c.get(key, 0)

    @property
    def duplicates(self) -> int:
        return sum(self._c.get(k, 0) for k in _DUP_OUTCOMES)

    @property
    def by_category(self) -> dict[str, int]:
        return dict(self._by_category)

    def as_dict(self) -> dict:
        return {
            **self._c,
            'duplicates_total': self.duplicates,
            'by_category': self.by_category,
        }

    def summary(self) -> str:
        c = self._c
        return (
            f"seen={c['seen']} new_url={c['new_urls']} "
            f"downloaded={c[DOWNLOADED]} "
            f"dup={self.duplicates}(L0:{c[DUP_URL]} L1:{c[DUP_L1]} "
            f"L2:{c[DUP_L2]} L3:{c[DUP_L3]} L4:{c[DUP_L4]}) "
            f"not_cctv={c[NOT_CCTV]} uncertain={c[UNCERTAIN]} no_event={c[NO_EVENT]} "
            f"no_frame={c[NO_FRAME]} portrait={c[PORTRAIT]} "
            f"failed={c[FAILED] + c[ERROR]}"
        )


class VideoPipeline:
    """
    Args:
        db          : DBManager
        downloader  : object có .fetch(url) -> str | None (tải vào staging, KHÔNG
                      chuyển đi) và thuộc tính .output_base (gốc dataset).
        classifier  : object có .assess_video(path) -> dict (CLIP, ưu tiên),
                      .assess_image(path) -> dict (Qwen) hoặc .is_cctv(url) -> bool.
                      None = bỏ qua bước phân loại. Xem load_classifier().
        platform    : tên platform để log/ghi DB khi URL không parse được
        category    : thư mục con trong <OUTPUT_BASE> (mặc định NAS pvn_share)
        skip_portrait: True = loại video 9:16.
                       Đặt False cho TikTok/Reels/Shorts nếu vẫn muốn giữ
                       clip CCTV được repost dạng dọc.
        resolve_short_links: True = theo redirect của vm.tiktok.com/fb.watch/...
                       trước khi tra L1, để short link và URL đầy đủ cùng
                       trỏ về một canonical ID.
        event_filter : object có .assess_video(path) -> {'is_event': bool, ...}
                       (VideoMAEEventFilter), chạy SAU mọi bộ lọc khác. None = tắt.
        dedup_content: True = loại video trùng nội dung (L2/L3/L4). False = vẫn
                       tính + lưu fingerprint (để lượt sau so được) nhưng KHÔNG
                       loại. L0/L1 luôn bật — tắt thì mỗi lượt xử lý lại mọi URL cũ.
        reject_action: 'move' / 'delete' cho MỌI lý do, hoặc dict theo từng lý do
                       loại / nhóm (xem action_for). Mặc định env CRAWL_REJECT_ACTION.
        rejected_base: gốc thư mục chứa video bị loại (mặc định REJECTED_BASE).
    """

    def __init__(
        self,
        db,
        downloader,
        classifier=None,
        platform: str | None = None,
        category: str = 'CCTV',
        skip_portrait: bool = True,
        resolve_short_links: bool = True,
        dedup_content: bool = True,
        event_filter=None,
        reject_action: str | dict | None = None,
        rejected_base: str | None = None,
    ):
        self.db                  = db
        self.downloader          = downloader
        self.classifier          = classifier
        self.platform            = platform
        self.category            = category
        self.skip_portrait       = skip_portrait
        self.resolve_short_links = resolve_short_links
        self.dedup_content       = dedup_content
        self.event_filter        = event_filter
        self.output_base         = downloader.output_base
        self.reject_action       = (
            {str(k): str(v).lower() for k, v in reject_action.items()}
            if isinstance(reject_action, dict) else (reject_action or REJECT_ACTION).lower())
        self.rejected_base       = rejected_base or REJECTED_BASE
        self.stats               = PipelineStats()

        rules = self.reject_action if isinstance(self.reject_action, dict) else {'*': self.reject_action}
        bad = {k: v for k, v in rules.items() if v not in ('move', 'delete')}
        if bad or REJECT_ACTION not in ('move', 'delete'):
            raise ValueError(
                f"reject_action không hợp lệ: {bad or REJECT_ACTION!r} — "
                f"chỉ nhận 'move' hoặc 'delete'"
            )
        if any(self.action_for(o) == 'move' for o in REJECT_GROUPS):
            # Tạo ngay đầu task: NAS chưa mount thì chết sớm với lỗi rõ ràng,
            # không đợi tới video bị loại đầu tiên.
            os.makedirs(self.rejected_base, exist_ok=True)

    # ── API chính ──────────────────────────────────────────────────────

    def process(self, url: str, source_page: str | None = None,
                category: str | None = None) -> str:
        """
        Xử lý 1 URL qua toàn bộ 4 tầng. Không bao giờ raise —
        lỗi bất ngờ trả về ERROR để 1 URL xấu không làm chết cả task.

        category: ghi đè self.category cho riêng URL này — dùng để lưu video
            vào thư mục theo nhãn sự việc (Arson, Robbery, RoadAccident...)
            tùy keyword nào tìm ra nó.
        """
        self.stats.bump('seen')
        try:
            return self._process(url, source_page, category or self.category)
        except Exception as e:
            logger.error(f"[Pipeline] Lỗi không mong đợi ({url}): {e}", exc_info=True)
            self.stats.bump(ERROR)
            self._finish(url, ERROR)
            return ERROR

    def process_batch(self, urls, source_page: str | None = None,
                      category: str | None = None, should_stop=None) -> dict:
        """
        Xử lý 1 batch URL, trả về {outcome: count} của riêng batch này.

        should_stop: callback không tham số, kiểm TRƯỚC MỖI URL. Trả True thì
            dừng batch giữa chừng và trả về phần đã xử lý.

            Cần thiết vì trần thời gian của task (CRAWL_TASK_TIME_BUDGET_MINUTES)
            trước đây chỉ kiểm được GIỮA các batch — trên GPU 6GB, VLM mất ~48s/URL
            nên 1 batch 25 URL ≈ 20 phút. Task hết giờ ở phút 60 vẫn phải chạy
            tiếp tới phút 80 mới nhả slot, và nếu execution_timeout tới trước thì
            bị kill giữa batch, mất sạch tiến độ chưa kịp log.
        """
        result: dict[str, int] = {}
        for url in urls:
            if should_stop is not None:
                try:
                    if should_stop():
                        break
                except Exception:
                    pass          # callback lỗi thì cứ chạy tiếp, đừng chết batch
            outcome = self.process(url, source_page, category)
            result[outcome] = result.get(outcome, 0) + 1
        return result

    # ── Luồng xử lý ────────────────────────────────────────────────────

    def _process(self, url: str, source_page: str | None, category: str) -> str:
        # ── Chốt định dạng URL ───────────────────────────────────────
        # Scraper phải yield URL TUYỆT ĐỐI. href trên trang thường là đường
        # dẫn tương đối ('/video/x8ewysm'); nếu scraper quên nối origin thì
        # trước đây lỗi chỉ lộ ra ở tận cùng (thumbnail fail → yt-dlp "is not
        # a valid URL" → Playwright "Cannot navigate to invalid URL"), rất
        # khó truy nguyên. Chặn tại cổng và nói rõ scraper nào sai.
        if not url.startswith(('http://', 'https://')):
            logger.error(
                f"[Pipeline] URL không tuyệt đối, bỏ qua: {url!r} "
                f"(source_page={source_page}) — scraper cần nối origin "
                f"vào href tương đối trước khi yield"
            )
            self.stats.bump(ERROR)
            return ERROR

        # ── Nhận diện platform + canonical ID ────────────────────────
        # Short link (vm.tiktok.com/xxx) chứa ID KHÁC canonical ID thật,
        # nên phải resolve redirect trước khi tra L1.
        lookup_url = url
        if self.resolve_short_links and dedup.is_short_link(url):
            lookup_url = dedup.resolve_short_url(url)

        canonical = dedup.extract_canonical_id(lookup_url)
        platform  = canonical[0] if canonical else (
            dedup.detect_platform(lookup_url) or self.platform
        )

        # ── L0: URL đã xử lý xong chưa? ──────────────────────────────
        # Ghi URL gốc (không phải URL đã resolve) để scraper nhận lại
        # đúng URL nó tìm thấy ở lần chạy sau.
        is_new = self.db.add_video_url(
            url, source_page=source_page, source_platform=platform
        )
        if is_new:
            self.stats.bump('new_urls')
        else:
            status = self.db.get_url_status(url)
            if status in _TERMINAL_STATUSES:
                logger.debug(f"[L0] Đã xử lý trước đó ({status}): {url}")
                self.stats.bump(DUP_URL)
                return DUP_URL

        ctx = {'url': url, 'resolved_url': lookup_url, 'source_page': source_page,
               'platform': platform, 'category': category}

        # ── L1: Canonical ID (trước khi tải — chắc chắn cùng 1 video) ─────
        if canonical:
            orig = self.db.find_by_canonical_id(*canonical)
            if orig:
                return self._reject(ctx, DUP_L1, {'dup_l1': {
                    'canonical_id': ':'.join(canonical), 'original': orig,
                }})
        else:
            logger.debug(f"[L1] Không parse được canonical ID: {url}")

        # ── Download vào staging (ổ nội bộ) — CHƯA vào dataset ──────────
        staged = self.downloader.fetch(lookup_url)
        if not staged:
            logger.info(f"[FAILED] Không tải được: {url}")
            self._finish(url, FAILED)
            self.stats.bump(FAILED)
            return FAILED

        try:
            return self._filter_and_store(ctx, staged, canonical)
        finally:
            # File đã được chuyển đi (dataset / thư mục loại) thì chỉ còn thư mục
            # tạm rỗng; chưa chuyển (delete, lỗi) thì xóa luôn cả file.
            release_staged(staged)

    def _filter_and_store(self, ctx: dict, staged: str, canonical) -> str:
        """Chạy các bộ lọc trên file đã tải, rồi chuyển vào dataset hoặc loại."""
        url, category = ctx['url'], ctx['category']
        checks: dict = {           # kết quả mọi bộ lọc đã chạy → filter_detail
            # Ghi lại bộ lọc nào đang bật cho video này: video nhận vào khi
            # cctv=False là CHƯA được kiểm tra CCTV — lọc lại được sau bằng
            # json_extract(filter_detail, '$.filters_enabled.cctv') = 0.
            'filters_enabled': {
                'cctv':     self.classifier is not None,
                'portrait': self.skip_portrait,
                'dedup':    self.dedup_content,
                'videomae': self.event_filter is not None,
            },
        }

        # ── Portrait 9:16 ────────────────────────────────────────────
        if self.skip_portrait:
            dims = dedup.video_dimensions(staged) or (0, 0)
            portrait = dedup.is_portrait_video(staged)
            checks['portrait'] = {
                'width': dims[0], 'height': dims[1], 'is_portrait': portrait,
            }
            if portrait:
                return self._reject(ctx, PORTRAIT, checks, staged)

        # ── L2: File hash ────────────────────────────────────────────
        file_hash = dedup.compute_file_hash(staged)
        if file_hash and self.dedup_content:
            orig = self.db.find_by_file_hash(file_hash)
            if orig:
                checks['dup_l2'] = {'sha256': file_hash, 'original': orig}
                return self._reject(ctx, DUP_L2, checks, staged)

        # ── L3: Thumbnail phash ──────────────────────────────────────
        duration   = dedup.get_video_duration(staged)
        checks['duration_s'] = round(duration, 1)
        thumb_hash = dedup.compute_thumbnail_hash(staged, duration)
        if thumb_hash and self.dedup_content:
            orig = self.db.find_by_thumbnail(thumb_hash, duration)
            if orig:
                checks['dup_l3'] = {'thumbnail_hash': thumb_hash, 'original': orig}
                return self._reject(ctx, DUP_L3, checks, staged)

        # ── L4: 5-frame fingerprint ──────────────────────────────────
        fingerprint = dedup.compute_fingerprint(staged, duration)
        if fingerprint and self.dedup_content:
            orig = self.db.find_by_fingerprint(fingerprint, duration)
            if orig:
                checks['dup_l4'] = {'fingerprint': fingerprint, 'original': orig}
                return self._reject(ctx, DUP_L4, checks, staged)

        # ── Phân loại CCTV (đắt nhất → chạy cuối) ────────────────────
        if self.classifier is not None:
            verdict, frame = self._run_classifier(staged, duration, ctx['resolved_url'])
            checks['classifier'] = verdict
            if verdict.get('is_cctv') is None:
                if verdict.get('invalid_image'):
                    # Mọi frame đều đen/hỏng — là 1 lý do loại thật, cần xem lại
                    return self._reject(ctx, NO_FRAME, checks, staged, frame)
                # Lỗi GPU/inference: KHÔNG phải quyết định của bộ lọc. Ghi failed
                # (không phải trạng thái cuối) để lượt sau thử lại.
                logger.warning(f"[Phân loại lỗi] {url}: {verdict.get('error')}")
                self._finish(url, ERROR, checks=checks)
                self.stats.bump(ERROR)
                return ERROR
            if verdict.get('label') == 'uncertain':
                return self._reject(ctx, UNCERTAIN, checks, staged, frame)
            if not verdict['is_cctv']:
                return self._reject(ctx, NOT_CCTV, checks, staged, frame)
        self.stats.bump('cctv')

        # Điểm từng đoạn VideoMAE cho file annotation — giữ ngoài `checks` (vào DB)
        ann_src = None

        # ── VideoMAE: có đoạn nào mang nhãn sự việc (khác Normal)? ──── chạy CUỐI
        # Nhãn crawl "Normal" (lớp âm tính) thì bỏ qua: bộ lọc này theo định nghĩa sẽ
        # loại MỌI video bình thường.
        if self.event_filter is not None and _same_label(category, 'Normal'):
            checks['videomae'] = {'skipped': 'nhãn Normal — không áp dụng bộ lọc sự việc'}
        elif self.event_filter is not None:
            ev = self.event_filter.assess_video(staged)
            ann_src = {'windows': ev.pop('windows', None), 'video': ev.pop('video', None),
                       'threshold': ev.get('threshold')}
            if ev.get('best'):
                # Chỉ để tham khảo, KHÔNG dùng để lọc: có đoạn nào đạt ngưỡng mang đúng
                # nhãn của từ khóa tìm ra video không (Armed_suspect ~ ArmedSuspect, Smok ~ Smoke)
                wins = ev.get('event_windows') or [ev['best']]
                try:        # tên hiển thị: "Gun-toting robber" ↔ thư mục GunRobber
                    from platform_crawlers.labels import display as _display
                    names = {category, _display(category)}
                except Exception:
                    names = {category}
                ev['matches_category'] = any(
                    _same_label(w.get('label', ''), n) for w in wins for n in names)
            checks['videomae'] = ev
            if ev.get('is_event') is None:
                logger.warning(f"[VideoMAE lỗi] {url}: {ev.get('error')}")
                self._finish(url, ERROR, checks=checks)
                self.stats.bump(ERROR)
                return ERROR
            if not ev['is_event']:
                return self._reject(ctx, NO_EVENT, checks, staged,
                                    annotation=(duration, ann_src))

        # ── Nhận → chuyển vào dataset + lưu cả 4 tầng ────────────────
        file_path = move_file(staged, os.path.join(self.output_base, category))
        if not file_path:
            self._finish(url, FAILED, checks=checks)
            self.stats.bump(FAILED)
            return FAILED
        self._write_annotation(file_path, duration, ann_src)

        p, otype, oid = canonical if canonical else (ctx['platform'], None, None)
        self.db.save_video_fingerprint(
            url=ctx['resolved_url'],
            file_path=file_path,
            duration=duration,
            source_platform=p,
            canonical_object_type=otype,
            canonical_object_id=oid,
            file_hash=file_hash,
            thumbnail_hash=thumb_hash,
            fingerprint=fingerprint,
        )
        self.db.update_url_status(
            url, 'downloaded', file_path=file_path, final_category=category,
            filter_detail=json.dumps(checks, ensure_ascii=False),
        )
        self.stats.bump(DOWNLOADED)
        self.stats.bump_category(category)
        logger.info(
            f"[✓ {category}] {os.path.basename(file_path)} "
            f"({duration:.0f}s) ← {ctx['platform'] or '?'} {url}"
        )
        return DOWNLOADED

    # ── Phân loại CCTV trên file đã tải ────────────────────────────────

    def _run_classifier(self, staged: str, duration: float, lookup_url: str):
        """
        Trả (verdict, frame_path). frame_path nằm trong thư mục staging nên
        release_staged() tự dọn; video bị loại thì ảnh được chuyển kèm.

        CLIP (assess_video): verdict là kết quả cả video — label, max_prob,
        cctv_seconds, top_segments... (xem CCTVVideoClassifier.assess_video).
        Qwen (assess_image) — verdict:
            {'is_cctv': bool, 'confidence', 'reason', 'frame_at_s', ...}
            {'is_cctv': None, 'invalid_image': True, ...}  mọi frame đen/hỏng
            {'is_cctv': None, 'error': '...'}               lỗi inference
        frame_path nằm trong thư mục staging nên release_staged() tự dọn; video
        bị loại thì frame được chuyển kèm để xem VLM đã nhìn thấy gì.
        """
        assess_video = getattr(self.classifier, 'assess_video', None)
        if assess_video is not None:
            thumbs = os.path.join(os.path.dirname(staged), 'thumbs')
            verdict = assess_video(staged, thumbs_dir=thumbs)
            frame = verdict.pop('frame', None)
            return verdict, (frame if frame and os.path.exists(frame) else None)

        assess = getattr(self.classifier, 'assess_image', None)
        if assess is None:
            # Classifier kiểu cũ chỉ có is_cctv(url) — đánh giá qua thumbnail
            try:
                ok = bool(self.classifier.is_cctv(lookup_url))
            except Exception as e:
                return {'is_cctv': None, 'error': f'{type(e).__name__}: {e}'}, None
            return {'is_cctv': ok, 'source': 'thumbnail'}, None

        stem  = os.path.splitext(os.path.basename(staged))[0]
        frame = os.path.join(os.path.dirname(staged), f'{stem}.frame.jpg')
        verdict = {'is_cctv': None, 'invalid_image': True,
                   'error': 'không trích được frame nào'}
        for pos in _VLM_FRAME_POSITIONS:
            ts = duration * pos if duration > 0 else 0.0
            if not dedup.extract_frame(staged, ts, frame):
                continue
            verdict = {**assess(frame), 'frame_at_s': round(ts, 1)}
            # Frame hỏng/đen → thử vị trí khác. Lỗi inference → dừng luôn,
            # thử frame khác cũng chỉ lỗi tiếp.
            if not verdict.get('invalid_image'):
                break
        return verdict, (frame if os.path.exists(frame) else None)

    # ── Helpers ────────────────────────────────────────────────────────

    def action_for(self, outcome: str) -> str:
        return action_for(outcome, self.reject_action)

    def _reject(self, ctx: dict, outcome: str, checks: dict,
                staged: str | None = None, frame: str | None = None,
                annotation: tuple | None = None) -> str:
        """
        Ghi video bị loại: DB (luôn luôn) + file (move sang rejected_base hoặc
        để release_staged xóa, tùy reject_action).
        """
        url = ctx['url']
        kept = None
        if staged and self.action_for(outcome) == 'move':
            dest_dir = os.path.join(self.rejected_base, outcome, ctx['category'])
            kept = move_file(staged, dest_dir)
            if kept:
                if frame:
                    move_file(frame, dest_dir)
                if annotation is not None:
                    # no_event: file annotation giống video được nhận, để mở trong công
                    # cụ gán nhãn mà sửa lại nếu VideoMAE loại nhầm
                    self._write_annotation(kept, *annotation)
                else:
                    self._write_sidecar(kept, ctx, outcome, checks)

        detail = (checks.get('videomae') if outcome == NO_EVENT else None) \
            or checks.get(outcome) or checks.get('classifier') or {}
        orig = detail.get('original') if isinstance(detail, dict) else None
        logger.info(
            f"[LOẠI {outcome}] {url} — {_REASON_TEXT.get(outcome, outcome)}"
            + (f"\n         ↳ đã có: {orig}" if orig else '')
            + (f"\n         ↳ {self._classifier_summary(detail)}"
               if outcome in (NOT_CCTV, UNCERTAIN, NO_EVENT) else '')
            + (f"\n         ↳ file: {kept}" if kept else '')
        )
        self._finish(url, outcome, file_path=kept, category=ctx['category'],
                     reject_reason=outcome, checks=checks)
        self.stats.bump(outcome)
        return outcome

    @staticmethod
    def _classifier_summary(v: dict) -> str:
        if 'is_event' in v:
            b = v.get('best') or {}
            return (f"VideoMAE cao nhất: {b.get('label')} {b.get('prob')} "
                    f"(Normal {b.get('normal_prob')}) ở {b.get('start_s')}–{b.get('end_s')}s, "
                    f"{v.get('n_windows')} đoạn")
        if 'max_prob' in v:
            top = (v.get('top_segments') or [{}])[0]
            return (f"label={v.get('label')} max_prob={v.get('max_prob')} "
                    f"cctv={v.get('cctv_seconds')}s/{v.get('duration')}s | "
                    f"đoạn cao nhất: {top.get('reasons', '—')}")
        return f"Qwen: {v.get('reason', '—')}"

    def _write_annotation(self, video_path: str, duration: float, src: dict | None):
        """<video>.json format video_classification (crawler_core/annotation.py)."""
        try:
            from crawler_core import annotation
            ef = self.event_filter
            data = annotation.build(
                video_path, duration, src,
                model_labels=getattr(ef, 'labels', None),
                model='VideoMAE')
            annotation.write(video_path, data)
        except Exception as e:      # annotation hỏng không được làm mất video đã nhận
            logger.warning(f"[Pipeline] Không tạo được annotation {video_path}: {e}")

    @staticmethod
    def _write_sidecar(video_path: str, ctx: dict, outcome: str, checks: dict):
        """<video>.json cạnh file bị loại — đọc được ngay trong trình duyệt file."""
        info = {
            'reason':      outcome,
            'reason_text': _REASON_TEXT.get(outcome, outcome),
            'rejected_at': datetime.now().isoformat(timespec='seconds'),
            **ctx,
            'filters':     checks,
        }
        path = os.path.splitext(video_path)[0] + '.json'
        try:
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(info, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"[Pipeline] Không ghi được {path}: {e}")

    def _finish(self, url: str, outcome: str, file_path: str | None = None,
                category: str | None = None, reject_reason: str | None = None,
                checks: dict | None = None):
        status = _STATUS_MAP.get(outcome)
        if status:
            try:
                self.db.update_url_status(
                    url, status, file_path=file_path, final_category=category,
                    reject_reason=reject_reason,
                    filter_detail=(json.dumps(checks, ensure_ascii=False)
                                   if checks else None),
                )
            except Exception as e:
                logger.warning(f"[Pipeline] Không ghi được status ({url}): {e}")
