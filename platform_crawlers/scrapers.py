"""
Scraper cho từng platform.

Mỗi scraper CHỈ có một nhiệm vụ: tìm ra URL video ứng viên và yield ra.
Không scraper nào được tự tải video — toàn bộ dedup/filter/download do
crawler_core.pipeline.VideoPipeline lo.

    class XyzScraper(BaseScraper):
        platform = 'xyz'
        def discover(self):
            yield ['https://...'], 'xyz:nguồn-nào', 'Arson'

discover() yield 3 giá trị: (urls, source_label, category)
    urls          danh sách URL video ứng viên
    source_label  nguồn nào tìm ra (ghi vào DB làm source_page)
    category      NHÃN SỰ VIỆC → video lưu vào <OUTPUT_BASE>/<category>/
                  (mặc định NAS pvn_share — crawler_core/downloader.py)
                  None = dùng category mặc định của pipeline

Thêm platform mới = thêm 1 class ở đây + 1 dòng vào _REGISTRY.
Dedup được đảm bảo tự động vì DAG luôn đẩy URL qua VideoPipeline.
"""

import logging
import os
from urllib.parse import quote

from platform_crawlers import config as cfg
from platform_crawlers import keywords
from platform_crawlers import label_cursor
from platform_crawlers.discovery import (
    ytdlp_discover,
    reddit_discover,
    facebook_search_discover,
    playwright_discover,
    discover_with_fallback,
    youtube_search_target,
    dailymotion_api_discover,
    dailymotion_paginated_discover,
    refresh_dailymotion_login,
)
from crawler_core.downloader import cookies_file_for, cookies_json_for

logger = logging.getLogger(__name__)


class BaseScraper:
    """
    platform      : tên platform (phải khớp key trong crawler_core.dedup)
    needs_cookies : True nếu không có cookie thì discovery chắc chắn thất bại
    label_full    : callback DAG truyền vào — label_full('Arson') → True nếu
                    nhãn đó đã đủ quota trong lượt chạy này. Xem iter_label_keywords().
    time_up       : callback DAG truyền vào — True nếu đã hết time budget.
    """
    platform: str = ''
    needs_cookies: bool = False

    def __init__(
        self,
        max_per_target: int | None = None,
        label_full=None,
        time_up=None,
        label_counts=None,
    ):
        self.max_per_target = max_per_target or cfg.MAX_PER_TARGET
        # Mặc định "không bao giờ đủ / không bao giờ hết giờ" để scraper vẫn
        # chạy được độc lập khi test tay ngoài Airflow.
        self._label_full = label_full or (lambda label: False)
        self._time_up    = time_up or (lambda: False)
        # {nhãn: số video đã có} — DAG truyền vào để sắp nhãn thiếu nhất lên
        # trước. None = không sắp theo thiếu hụt (test tay ngoài Airflow).
        self._label_counts = label_counts

    # ── Override ở subclass ────────────────────────────────────────────

    def discover(self):
        """
        Yield (urls, source_label, category) cho từng nguồn.
        category là nhãn sự việc (Arson/Robbery/...) hoặc None.
        """
        raise NotImplementedError

    # ── Dùng chung ─────────────────────────────────────────────────────

    def label_full(self, label) -> bool:
        """Nhãn này đã đủ quota chưa? Callback lỗi → coi như chưa đủ (chạy tiếp)."""
        try:
            return bool(self._label_full(label))
        except Exception:
            return False

    def time_up(self) -> bool:
        try:
            return bool(self._time_up())
        except Exception:
            return False

    def iter_label_keywords(self, by_label: dict):
        """
        Sinh (label, keyword) — thay cho `for label, kws in by_label.items()`.

        Làm 3 việc mà vòng lặp thẳng không làm được:

        1. SẮP NHÃN THEO THIẾU HỤT (platform_crawlers.label_cursor): nhãn ít video
           nhất đi trước, nhãn nhiều lượt liên tiếp không ra video bị đẩy xuống
           cuối. Vòng lặp thẳng luôn bắt đầu từ Abuse nên với trần thời gian, các
           nhãn cuối bảng không bao giờ tới lượt.

        2. BỎ QUA DISCOVERY CỦA NHÃN ĐÃ ĐỦ QUOTA. Trước đây quota nhãn chỉ được
           kiểm tra Ở DAG, tức là SAU KHI scraper đã cuộn Playwright xong ~1
           phút — kết quả bị ném đi kèm log "Nhãn X đã đủ 30/30 — bỏ qua".
           Vì MAX_PER_LABEL từng nhỏ hơn MAX_PER_TARGET (30 < 40), từ khóa thứ
           2 và 3 của MỌI nhãn là công toi 100%. Riêng Reddit: 1 nhãn = 3 từ
           khóa × 6 nguồn = 18 lượt discover() mà 17 lượt bị ném đi.

        3. DỪNG KHI HẾT GIỜ, trước khi tốn thêm 1 lượt discovery mà DAG sẽ bỏ.

        Subclass chỉ cần: `for label, kw in self.iter_label_keywords(by_label):`
        """
        order = label_cursor.order_labels(
            self.platform, list(by_label), self._label_counts
        )

        for label in order:
            if self.time_up():
                # KHÔNG cần "nhích con trỏ" như thiết kế cũ: lượt sau tính lại thứ
                # tự từ số video thật trong DB, nên nhãn dở dang vẫn đang thiếu và
                # vẫn được ưu tiên. Tự phục hồi, không có gì để kẹt.
                logger.info(
                    f"[{self.platform}] Hết time budget — dừng discovery trước "
                    f"nhãn {label}"
                )
                return

            if self.label_full(label):
                logger.info(
                    f"[{self.platform}] Nhãn {label} đã đủ quota — bỏ qua "
                    f"{len(by_label[label])} lượt discovery (không cuộn vô ích)"
                )
                continue

            label_cursor.save_current(self.platform, label)

            for kw in by_label[label]:
                if self.time_up():
                    logger.info(
                        f"[{self.platform}] Hết time budget giữa nhãn {label}"
                    )
                    return
                if self.label_full(label):
                    logger.info(
                        f"[{self.platform}] Nhãn {label} vừa đủ quota — bỏ các "
                        f"từ khóa còn lại của nhãn"
                    )
                    break
                yield label, kw

    def cookies(self) -> str | None:
        return cookies_file_for(self.platform)

    def unavailable_reason(self) -> str | None:
        """Trả lý do không chạy được, None nếu sẵn sàng."""
        if self.needs_cookies and not self.cookies():
            return (
                f'thiếu cookie đăng nhập. Đăng nhập {self.platform} trong Chrome '
                f'thường của bạn rồi chạy: '
                f'python scripts/import_browser_cookies.py {self.platform}'
            )
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# YouTube — có search extractor sẵn (ytsearchN:), không cần cookie
# ═══════════════════════════════════════════════════════════════════════════════

class YouTubeScraper(BaseScraper):
    platform = 'youtube'

    def discover(self):
        # Gọi TRONG discover() (tức trong task), không ở module level —
        # Airflow parse file DAG mỗi ~30s, sinh keyword lúc import sẽ đốt quota.
        by_label = keywords.phrases_by_label(cfg.YOUTUBE_KEYWORDS_OVERRIDE)
        # YouTube KHÔNG có trang để cuộn — ytsearchN lấy đúng N kết quả trong 1
        # request. "Load thêm URL" ở đây = tăng CRAWL_MAX_PER_TARGET, không liên
        # quan tới CRAWL_MAX_SCROLLS.
        for label, kw in self.iter_label_keywords(by_label):
            urls = ytdlp_discover(
                youtube_search_target(kw, self.max_per_target),
                limit=self.max_per_target,
                platform=self.platform,
                cookies_file=self.cookies(),
            )
            yield urls, f'youtube:{label or "adhoc"}:{kw}', label


# ═══════════════════════════════════════════════════════════════════════════════
# Dailymotion — search qua URL /search/<query>/videos
# ═══════════════════════════════════════════════════════════════════════════════

class DailymotionScraper(BaseScraper):
    platform = 'dailymotion'

    def discover(self):
        # Dailymotion search KHÔNG lazy-load khi cuộn (đã kiểm chứng thực tế:
        # scrollHeight không đổi dù cuộn hết trang) — phải phân trang qua
        # ?page=N. Cần cookie đăng nhập (dailymotion_cookies.json), nếu
        # không có sẽ chỉ thấy "no results" + gợi ý trending không liên quan.
        #
        # access_token chỉ sống ~10-24h — tự làm mới bằng refresh_token
        # TRƯỚC MỖI LƯỢT chạy task (không phải mỗi từ khóa) để không phải
        # đăng nhập tay lại hàng ngày. Thất bại (refresh_token hết hạn ~3
        # tháng) thì tiếp tục chạy với cookie cũ — vẫn còn hạn dùng tạm nếu
        # chưa hết, log đã cảnh báo rõ cách khắc phục.
        refresh_dailymotion_login(cookies_json_for(self.platform))

        by_label = keywords.phrases_by_label(cfg.DAILYMOTION_KEYWORDS_OVERRIDE)
        for label, kw in self.iter_label_keywords(by_label):
            urls = []
            if cfg.DAILYMOTION_USE_API:
                # Đường CHÍNH: API công khai, không cần cookie, 100 URL/request.
                # Trang search HTML trần cứng 20 URL/từ khóa (xem
                # dailymotion_api_discover) nên đường này hơn hẳn.
                urls = dailymotion_api_discover(
                    kw,
                    limit=self.max_per_target,
                    max_duration_s=cfg.max_duration_for(self.platform),
                    max_pages=cfg.DAILYMOTION_API_MAX_PAGES,
                )
            if not urls:
                # Fallback: quét HTML (cần cookie login). Giữ lại để nếu API bị
                # chặn/đổi thì DAG vẫn còn đường chạy, không đứng hẳn.
                if cfg.DAILYMOTION_USE_API:
                    logger.info(
                        f"[dailymotion] API không ra URL cho '{kw}' — thử quét HTML"
                    )
                urls = dailymotion_paginated_discover(
                    kw,
                    limit=self.max_per_target,
                    cookies_json=cookies_json_for(self.platform),
                    max_pages=cfg.DAILYMOTION_MAX_PAGES,
                    timeout_s=cfg.DISCOVERY_TIMEOUT,
                )
            yield urls, f'dailymotion:{label or "adhoc"}:{kw}', label


# ═══════════════════════════════════════════════════════════════════════════════
# TikTok — hashtag page (tiktok:tag extractor). Cookie giúp giảm bị chặn.
# ═══════════════════════════════════════════════════════════════════════════════

class TikTokScraper(BaseScraper):
    platform = 'tiktok'
    # Đã kiểm tra 2026-07: trang /tag/ chặn khách ẩn danh (Playwright ra 0 link)
    # và extractor tiktok của yt-dlp báo "marked as broken / No working app info".
    # Cả hai đường đều cần cookie đăng nhập.
    needs_cookies = True

    def discover(self):
        by_label = keywords.hashtags_by_label(cfg.TIKTOK_HASHTAGS_OVERRIDE)
        for label, tag in self.iter_label_keywords(by_label):
            page = f'https://www.tiktok.com/tag/{quote(tag)}'
            urls = discover_with_fallback(
                ytdlp_target=page,
                page_url=page,
                link_pattern=r'/@[\w.\-]+/video/\d+',
                platform=self.platform,
                limit=self.max_per_target,
                cookies_file=self.cookies(),
                cookies_json=cookies_json_for(self.platform),
            )
            yield urls, f'tiktok:{label or "adhoc"}:#{tag}', label


# ═══════════════════════════════════════════════════════════════════════════════
# Instagram — hashtag page. BẮT BUỘC cookie đăng nhập.
# ═══════════════════════════════════════════════════════════════════════════════

class InstagramScraper(BaseScraper):
    platform = 'instagram'
    needs_cookies = True

    def discover(self):
        by_label = keywords.hashtags_by_label(cfg.INSTAGRAM_HASHTAGS_OVERRIDE)
        for label, tag in self.iter_label_keywords(by_label):
            page = f'https://www.instagram.com/explore/tags/{quote(tag)}/'
            urls = discover_with_fallback(
                ytdlp_target=page,
                page_url=page,
                link_pattern=r'/(?:p|reel)/[A-Za-z0-9_\-]+',
                platform=self.platform,
                limit=self.max_per_target,
                cookies_file=self.cookies(),
                cookies_json=cookies_json_for(self.platform),
            )
            yield urls, f'instagram:{label or "adhoc"}:#{tag}', label


# ═══════════════════════════════════════════════════════════════════════════════
# Reddit — public JSON API, không cần auth
# ═══════════════════════════════════════════════════════════════════════════════

class RedditScraper(BaseScraper):
    platform = 'reddit'

    def discover(self):
        # Search theo keyword của từng nhãn để video được gán nhãn đúng.
        # Search toàn Reddit trước (rộng nhất, 1 request/keyword), rồi mới
        # search trong từng subreddit đã cấu hình.
        by_label = keywords.phrases_by_label(None)

        # Reddit là platform bị lãng phí nặng nhất nếu không kiểm tra quota giữa
        # các NGUỒN: 1 nhãn = 3 từ khóa × (1 toàn Reddit + 5 subreddit) = 18 lượt
        # discover(), mỗi lượt là 1 phiên Playwright cuộn tab Media. Quota nhãn
        # bị lấp đầy ngay lượt đầu ⇒ 17 lượt sau chạy xong rồi bị DAG ném đi.
        # Vì vậy phải kiểm tra label_full() cả trong vòng lặp subreddit, không
        # chỉ ở iter_label_keywords().
        for label, kw in self.iter_label_keywords(by_label):
            if cfg.REDDIT_GLOBAL_SEARCH:
                urls = reddit_discover(
                    None, query=kw, limit=self.max_per_target
                )
                yield urls, f'reddit:all:{kw}', label

            for sub in cfg.REDDIT_SUBREDDITS:
                if self.label_full(label) or self.time_up():
                    logger.info(
                        f"[reddit] Nhãn {label} đủ quota/hết giờ — bỏ các "
                        f"subreddit còn lại của từ khóa '{kw}'"
                    )
                    break
                urls = reddit_discover(
                    sub, query=kw, limit=self.max_per_target
                )
                yield urls, f'reddit:r/{sub}:{kw}', label


# ═══════════════════════════════════════════════════════════════════════════════
# Facebook — search tab "Thước phim" (/search/videos/). BẮT BUỘC cookie.
# ═══════════════════════════════════════════════════════════════════════════════

class FacebookScraper(BaseScraper):
    platform = 'facebook'
    needs_cookies = True

    def unavailable_reason(self) -> str | None:
        # Discovery chạy bằng Playwright nên cần cookie JSON, KHÔNG phải
        # Netscape của yt-dlp — kiểm tra đúng file sẽ dùng.
        if not cookies_json_for(self.platform):
            return (
                'Facebook yêu cầu đăng nhập để search. Đăng nhập facebook.com '
                'trong Chrome thường rồi chạy: '
                'python scripts/import_browser_cookies.py facebook'
            )
        return None

    def discover(self):
        by_label = keywords.phrases_by_label(cfg.FACEBOOK_KEYWORDS_OVERRIDE)
        for label, kw in self.iter_label_keywords(by_label):
            urls = facebook_search_discover(
                kw,
                limit=self.max_per_target,
                cookies_json=cookies_json_for(self.platform),
                max_scrolls=cfg.FACEBOOK_MAX_SCROLLS,
                timeout_s=cfg.DISCOVERY_TIMEOUT,
            )
            yield urls, f'facebook:{label or "adhoc"}:{kw}', label


# ═══════════════════════════════════════════════════════════════════════════════
# Vimeo — KHÔNG có search extractor → phải chỉ định channel/user cụ thể
# ═══════════════════════════════════════════════════════════════════════════════

class VimeoScraper(BaseScraper):
    platform = 'vimeo'

    def unavailable_reason(self) -> str | None:
        if not cfg.VIMEO_TARGETS:
            return (
                'Vimeo không có search extractor trong yt-dlp — cần chỉ định '
                'channel/user cụ thể qua env CRAWL_VIMEO_TARGETS='
                '"https://vimeo.com/channels/abc|https://vimeo.com/someuser"'
            )
        return super().unavailable_reason()

    def discover(self):
        # Vimeo chỉ liệt kê được channel/user → KHÔNG biết video thuộc nhãn
        # nào. Trả category=None để pipeline dùng thư mục mặc định; muốn gán
        # nhãn thì phải phân loại lại thủ công sau.
        for target in cfg.VIMEO_TARGETS:
            urls = ytdlp_discover(
                target,
                limit=self.max_per_target,
                platform=self.platform,
                cookies_file=self.cookies(),
            )
            yield urls, f'vimeo:{target}', None


# ═══════════════════════════════════════════════════════════════════════════════
# X / Twitter — yt-dlp không liệt kê được timeline → Playwright + cookie
# ═══════════════════════════════════════════════════════════════════════════════

class XScraper(BaseScraper):
    platform = 'x'
    needs_cookies = True

    def unavailable_reason(self) -> str | None:
        # Playwright cần cookie JSON (khác định dạng Netscape của yt-dlp)
        if not cookies_json_for(self.platform):
            return (
                'X yêu cầu đăng nhập để search. Đăng nhập x.com trong Chrome '
                'thường rồi chạy: python scripts/import_browser_cookies.py x'
            )
        return None

    def discover(self):
        pattern      = r'/status/\d+'
        cookies_json = cookies_json_for(self.platform)

        # Nối 'filter:videos' (cú pháp search của X) để chỉ lấy tweet có video.
        suffix   = f' {cfg.X_SEARCH_SUFFIX}'.rstrip()
        by_label = keywords.phrases_by_label(cfg.X_QUERIES_OVERRIDE)

        for label, kw in self.iter_label_keywords(by_label):
            # Query chỉ định tay qua env giữ nguyên, không nối thêm suffix
            q = kw if cfg.X_QUERIES_OVERRIDE else f'{kw}{suffix}'
            # Tab MEDIA (f=media), KHÔNG phải Latest (f=live). Đã đo thực
            # tế cùng query 'cctv robbery filter:videos', cùng cookie:
            #     f=media  → 100 tweet (chạm trần limit), 11s
            #     f=live   →  49 tweet,                   32s
            #     (mặc định Top) → 49 tweet,              29s
            # Media có riêng 75 tweet mà Latest không có. Nhanh hơn vì
            # bố cục lưới nên mỗi lần cuộn ra nhiều link hơn hẳn.
            #
            # ⚠ ĐỪNG thêm pf=/lf= vào URL này. Panel Filter của X có
            # People (From anyone | People you follow) và Location
            # (Anywhere | Near you); cả hai mặc định là lựa chọn RỘNG
            # NHẤT và được biểu đạt bằng cách KHÔNG có tham số. X không
            # nhận giá trị 'off' — đã đo thực tế 2026-07-31:
            #     (không pf/lf)  → 100 tweet   ← đang dùng, đúng ý
            #     pf=off&lf=off  →   0 tweet   "No results for..."
            #     pf=off         →   0 tweet   "Something went wrong"
            #     lf=on          →  58 tweet   (Near you — thu hẹp thật)
            urls = playwright_discover(
                f'https://x.com/search?q={quote(q)}&f={cfg.X_SEARCH_TAB}',
                link_pattern=pattern,
                limit=self.max_per_target,
                cookies_json=cookies_json,
                max_scrolls=cfg.MAX_SCROLLS,
                timeout_s=cfg.DISCOVERY_TIMEOUT,
            )
            yield urls, f'x:{label or "adhoc"}:{kw}', label

        # Timeline của account cụ thể → không biết nhãn
        for account in cfg.X_ACCOUNTS:
            if self.time_up():
                break
            urls = playwright_discover(
                f'https://x.com/{account}',
                link_pattern=pattern,
                limit=self.max_per_target,
                cookies_json=cookies_json,
                max_scrolls=cfg.MAX_SCROLLS,
                timeout_s=cfg.DISCOVERY_TIMEOUT,
            )
            yield urls, f'x:account:{account}', None


# ═══════════════════════════════════════════════════════════════════════════════
# Registry
# ═══════════════════════════════════════════════════════════════════════════════

_REGISTRY: dict[str, type[BaseScraper]] = {
    'youtube':     YouTubeScraper,
    'tiktok':      TikTokScraper,
    'instagram':   InstagramScraper,
    'x':           XScraper,
    'reddit':      RedditScraper,
    'facebook':    FacebookScraper,
    'vimeo':       VimeoScraper,
    'dailymotion': DailymotionScraper,
}

ALL_PLATFORMS = tuple(_REGISTRY)


def get_scraper(platform: str, **kwargs) -> BaseScraper:
    if platform not in _REGISTRY:
        raise ValueError(
            f"Platform không hỗ trợ: {platform}. Có: {', '.join(ALL_PLATFORMS)}"
        )
    return _REGISTRY[platform](**kwargs)
