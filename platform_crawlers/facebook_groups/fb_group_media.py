"""
Scrape video URLs from the Media → Videos section of Facebook groups
that the user has already joined.
"""

import re
import logging
from .fb_session import FBSession

logger = logging.getLogger(__name__)
FB_BASE = 'https://www.facebook.com'

VIDEO_PATTERNS = [
    r'facebook\.com/watch\?',
    r'facebook\.com/reel/',
    r'facebook\.com/share/v/',
    r'facebook\.com/.+/videos/\d+',
    r'story_fbid=\d+',
]


class FBGroupMediaScraper:
    def __init__(self, session: FBSession):
        self.session = session
        self.page = session.page

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def get_joined_groups(self) -> list[str]:
        """
        Navigate to facebook.com/groups/?category=joined and return
        a list of group URLs the user has joined.
        """
        self.page.goto(
            f'{FB_BASE}/groups/?category=joined',
            wait_until='domcontentloaded'
        )
        self.session.random_delay(3, 5)

        seen, groups = set(), []

        for _ in range(12):        # scroll để load hết danh sách nhóm
            for link in self.page.query_selector_all('a[href*="/groups/"]'):
                href = (link.get_attribute('href') or '').split('?')[0].rstrip('/')
                if self._is_group_url(href) and href not in seen:
                    seen.add(href)
                    groups.append(href)
            self.session.scroll_page(times=1, pause=2.0)

        logger.info(f"Found {len(groups)} joined group(s)")
        return groups

    def get_group_videos(
        self,
        group_url: str,
        max_results: int = 200,
        known_urls: set = None,
        stop_after_known: int = 5,
        deep_scan: bool = False,
    ) -> tuple[list[str], bool]:
        """
        Open the group's Media → Videos tab and collect video post URLs.

        deep_scan=False (default / 6h run):
            Dừng khi gặp `stop_after_known` URL liên tiếp đã biết.
            Chỉ bắt video mới ở đầu danh sách.

        deep_scan=True (one-time full scan):
            Bỏ qua `consecutive_known` stop, scroll đến hết trang hoặc
            đạt max_results. Dùng để thu thập video cũ chưa từng được lấy.

        Returns (new_urls, scan_complete):
            scan_complete=True khi nhóm đã được scroll đến đáy (hoặc
            consecutive_known dừng tự nhiên trong fast mode).
        """
        media_url = group_url.rstrip('/') + '/media/videos'
        logger.info(f"Scraping: {media_url} (deep_scan={deep_scan})")

        self.page.goto(media_url, wait_until='domcontentloaded')
        self.session.random_delay(3, 5)

        if '/media/videos' not in self.page.url and 'groups' not in self.page.url:
            logger.warning(f"Redirected away from media tab: {self.page.url}")
            return [], False

        known_urls = known_urls or set()
        seen, video_urls = set(), []
        consecutive_known = 0
        scan_complete = False
        # Hệ số 4 đảm bảo đủ scroll; điều kiện dừng sớm sẽ thoát trước khi đạt giới hạn.
        max_scrolls = max(40, max_results // 4)

        # Số lần scroll liên tiếp không thấy link mới → mới kết luận đã đến đáy.
        # Tránh false positive khi mạng chậm chưa load kịp content.
        EMPTY_SCROLLS_NEEDED = 5
        empty_scroll_streak = 0
        prev_seen_count = -1

        for _ in range(max_scrolls):
            if len(video_urls) >= max_results:
                break

            for link in self.page.query_selector_all('a[href]'):
                href = link.get_attribute('href') or ''
                clean = self._to_video_url(href)
                if not clean or clean in seen:
                    continue
                seen.add(clean)

                if clean in known_urls:
                    if not deep_scan:
                        consecutive_known += 1
                else:
                    consecutive_known = 0
                    video_urls.append(clean)

            # Fast mode: dừng khi gặp đủ URL cũ liên tiếp
            if not deep_scan and consecutive_known >= stop_after_known:
                logger.info(
                    f"  → Gặp {consecutive_known} URL cũ liên tiếp — dừng sớm"
                )
                scan_complete = True
                break

            # Phát hiện đáy trang: cần N lần scroll liên tiếp không có link mới
            if len(seen) == prev_seen_count:
                empty_scroll_streak += 1
                if empty_scroll_streak >= EMPTY_SCROLLS_NEEDED:
                    logger.info(
                        f"  → {EMPTY_SCROLLS_NEEDED} lần scroll không thấy link mới "
                        f"— đã đến đáy trang"
                    )
                    scan_complete = True
                    break
                # Chờ 30s bằng micro-scroll để page không bị idle (Chromium đóng tab sau ~2-3 phút)
                for _ in range(6):
                    self.session.random_delay(5, 5)
                    try:
                        self.page.evaluate('window.scrollBy(0, 1)')
                    except Exception:
                        break
            else:
                empty_scroll_streak = 0
            prev_seen_count = len(seen)

            self.session.scroll_page(times=1, pause=2.5)

        logger.info(
            f"  → {len(video_urls)} URL mới, "
            f"{len(seen) - len(video_urls)} URL cũ, "
            f"scan_complete={scan_complete}"
        )
        return video_urls[:max_results], scan_complete

    def iter_group_videos(
        self,
        group_url: str,
        batch_size: int = 100,
        known_urls: set = None,
        stop_after_known: int = 5,
        deep_scan: bool = False,
    ):
        """
        Generator — mở trang Media/Videos của nhóm MỘT LẦN rồi yield từng
        batch `batch_size` URL mới. Giữ trang mở giữa các batch để không
        phải re-navigate và re-scroll từ đầu.

        Yield: (batch: list[str], is_last: bool)
            is_last=True khi đây là batch cuối (đã đến đáy hoặc early-stop).
        """
        media_url = group_url.rstrip('/') + '/media/videos'
        logger.info(f"Streaming: {media_url} (batch_size={batch_size}, deep_scan={deep_scan})")

        self.page.goto(media_url, wait_until='domcontentloaded')
        self.session.random_delay(3, 5)

        if '/media/videos' not in self.page.url and 'groups' not in self.page.url:
            logger.warning(f"Redirected away from media tab: {self.page.url}")
            return

        known_urls = set(known_urls or set())
        seen      = set()
        pending   = []           # URL đã thu nhưng chưa yield
        consecutive_known  = 0
        empty_scroll_streak = 0
        prev_seen_count    = -1
        batch_num          = 0

        while True:
            # Thu thập tất cả link hiện có trên trang
            for link in self.page.query_selector_all('a[href]'):
                href  = link.get_attribute('href') or ''
                clean = self._to_video_url(href)
                if not clean or clean in seen:
                    continue
                seen.add(clean)

                if clean in known_urls:
                    if not deep_scan:
                        consecutive_known += 1
                else:
                    consecutive_known = 0
                    pending.append(clean)
                    known_urls.add(clean)   # tránh thu trùng lần sau

            # Yield khi đủ batch
            while len(pending) >= batch_size:
                batch_num += 1
                batch = pending[:batch_size]
                pending = pending[batch_size:]
                logger.info(f"  Batch #{batch_num}: {len(batch)} URL mới")
                yield batch, False

            # Fast mode: đủ URL cũ liên tiếp → đây là batch cuối
            if not deep_scan and consecutive_known >= stop_after_known:
                logger.info(f"  → {consecutive_known} URL cũ liên tiếp — batch cuối")
                if pending:
                    batch_num += 1
                    logger.info(f"  Batch #{batch_num} (cuối): {len(pending)} URL mới")
                    yield pending, True
                else:
                    yield [], True
                return

            # Phát hiện đáy trang
            if len(seen) == prev_seen_count:
                empty_scroll_streak += 1
                if empty_scroll_streak >= 3:
                    logger.info(f"  → Đã đến đáy trang sau {empty_scroll_streak} scroll trống")
                    if pending:
                        batch_num += 1
                        logger.info(f"  Batch #{batch_num} (cuối): {len(pending)} URL mới")
                        yield pending, True
                    else:
                        yield [], True
                    return
                # Mạng chậm → chờ 30s và giữ page alive bằng scroll nhỏ.
                # Không dùng sleep dài vì Chromium headless đóng tab sau ~2-3 phút idle.
                logger.info(
                    f"  → Scroll #{empty_scroll_streak} không load thêm link, "
                    f"chờ 30s..."
                )
                for _ in range(6):      # 6 × 5s = 30s, page không bị idle
                    self.session.random_delay(5, 5)
                    try:
                        self.page.evaluate('window.scrollBy(0, 1)')   # micro-scroll giữ alive
                    except Exception:
                        break
            else:
                empty_scroll_streak = 0

            prev_seen_count = len(seen)
            self.session.scroll_page(times=1, pause=2.5)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _is_group_url(self, href: str) -> bool:
        return bool(re.search(r'facebook\.com/groups/[\w.\-]+$', href))

    def _to_video_url(self, href: str) -> str | None:
        if not href:
            return None
        if href.startswith('/'):
            href = FB_BASE + href
        if any(re.search(p, href) for p in VIDEO_PATTERNS):
            return href
        return None
