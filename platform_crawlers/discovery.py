"""
Discovery — tìm URL video ứng viên trên từng platform.

Ba cơ chế, chọn theo khả năng thực tế của từng platform:

  ytdlp_discover()      yt-dlp extract_flat — KHÔNG cần browser.
                        Dùng cho: YouTube (ytsearch), Dailymotion (search URL),
                        TikTok (tag), Instagram (tag), Vimeo (channel/user).

  reddit_discover()     Reddit public JSON API — không cần auth, không cần browser.

  playwright_discover() Scroll trang và gom link theo regex.
                        Dùng cho X/Twitter vì yt-dlp không có extractor
                        liệt kê timeline/search.

Mọi hàm đều trả list rỗng khi lỗi — không bao giờ raise, để 1 platform chết
không làm sập cả DAG.
"""

import logging
import os
import re
import time
from urllib.parse import quote

logger = logging.getLogger(__name__)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Khi entry chỉ có 'id', dựng URL đầy đủ theo template của platform
_URL_TEMPLATES = {
    'youtube':     'https://www.youtube.com/watch?v={id}',
    'dailymotion': 'https://www.dailymotion.com/video/{id}',
    'vimeo':       'https://vimeo.com/{id}',
    'instagram':   'https://www.instagram.com/p/{id}/',
}


# ═══════════════════════════════════════════════════════════════════════════════
# yt-dlp flat discovery
# ═══════════════════════════════════════════════════════════════════════════════

def _entry_to_url(entry: dict, platform: str | None) -> str | None:
    """Lấy URL đầy đủ từ 1 entry của extract_flat."""
    for key in ('webpage_url', 'original_url', 'url'):
        val = entry.get(key)
        if val and isinstance(val, str) and val.startswith('http'):
            return val

    vid = entry.get('id')
    if vid and platform in _URL_TEMPLATES:
        return _URL_TEMPLATES[platform].format(id=vid)
    return None


def ytdlp_discover(
    target: str,
    limit: int = 40,
    platform: str | None = None,
    cookies_file: str | None = None,
) -> list[str]:
    """
    Liệt kê URL video từ 1 target yt-dlp (search query, tag page, channel...).

    target ví dụ:
        'ytsearch40:cctv footage theft'
        'https://www.dailymotion.com/search/cctv%20theft/videos'
        'https://www.tiktok.com/tag/cctvfootage'
        'https://www.instagram.com/explore/tags/cctvfootage/'
        'https://vimeo.com/channels/somechannel'
    """
    import yt_dlp

    opts = {
        'quiet': True,
        'no_warnings': True,
        'noprogress': True,
        'skip_download': True,
        # extract_flat: chỉ lấy danh sách, KHÔNG resolve từng video (nhanh hơn nhiều)
        'extract_flat': 'in_playlist',
        'playlistend': limit,
        'ignoreerrors': True,
        'socket_timeout': 30,
    }
    if cookies_file and os.path.exists(cookies_file):
        opts['cookiefile'] = cookies_file

    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(target, download=False)
    except Exception as e:
        logger.warning(f"[Discovery] yt-dlp thất bại ({target}): {e}")
        return []

    if not info:
        return []

    entries = info.get('entries')
    if entries is None:
        # Target trỏ trực tiếp tới 1 video, không phải playlist
        single = _entry_to_url(info, platform)
        return [single] if single else []

    urls, seen = [], set()
    for entry in entries:
        if not entry:
            continue
        # Bỏ qua live stream ngay từ bước liệt kê
        if entry.get('is_live') or entry.get('live_status') in ('is_live', 'is_upcoming'):
            continue
        url = _entry_to_url(entry, platform)
        if url and url not in seen:
            seen.add(url)
            urls.append(url)
        if len(urls) >= limit:
            break

    logger.info(f"[Discovery] {target} → {len(urls)} URL")
    return urls


def youtube_search_target(keyword: str, limit: int) -> str:
    """YouTube có search extractor sẵn: ytsearchN:<keyword>."""
    return f'ytsearch{limit}:{keyword}'


def dailymotion_search_target(keyword: str) -> str:
    """Dailymotion dùng search URL (không có SEARCH_KEY như ytsearch)."""
    return f'https://www.dailymotion.com/search/{quote(keyword)}/videos'


# client_id/secret CÔNG KHAI của yt-dlp (hardcode trong yt-dlp/extractor/dailymotion.py)
# — chỉ dùng để đổi refresh_token (đã thuộc về user thật) lấy access_token
# mới, KHÔNG phải xin token ẩn danh (đó là con đường bị rate-limit toàn cầu,
# xem crawler_core/downloader.py). Endpoint refresh_token vẫn cần đúng
# client_id đã cấp ra refresh_token đó, nên buộc phải dùng lại giá trị này —
# đã kiểm chứng thực tế hoạt động đúng, KHÔNG rơi về chế độ ẩn danh.
_DM_CLIENT_ID     = 'f1a362d288c1b98099c7'
_DM_CLIENT_SECRET = 'eea605b96e01c796ff369935357eca920c5da4c5'


def refresh_dailymotion_login(cookies_json_path: str) -> bool:
    """
    Tự làm mới access_token đăng nhập Dailymotion bằng refresh_token, KHÔNG
    cần con người đăng nhập lại. access_token chỉ sống ~10-24h nên nếu không
    làm mới, sau chừng đó thời gian search sẽ ÂM THẦM quay về chế độ ẩn danh
    (xem dailymotion_paginated_discover — Dailymotion không báo lỗi, chỉ trả
    "no results" + gợi ý trending không liên quan).

    QUAN TRỌNG: refresh_token XOAY VÒNG (rotate) — mỗi lần refresh thành
    công, server trả về 1 refresh_token MỚI và vô hiệu hóa refresh_token cũ.
    Bắt buộc phải ghi đè lại file cookie ngay, nếu không lần refresh kế tiếp
    sẽ thất bại vì dùng refresh_token đã bị thu hồi.

    Gọi 1 lần đầu mỗi lượt discover() (không phải mỗi từ khóa) — đủ cho cả
    lượt chạy vì access_token sống hơn thời gian 1 task chạy thường thấy.

    Trả True nếu làm mới thành công, False nếu cần đăng nhập lại thủ công
    (refresh_token đã hết hạn ~3 tháng, hoặc bị thu hồi, hoặc chưa có cookie).
    """
    import json as _json

    if not cookies_json_path or not os.path.exists(cookies_json_path):
        return False
    try:
        with open(cookies_json_path) as f:
            cookies = _json.load(f)
    except Exception as e:
        logger.warning(f"[Dailymotion] Không đọc được cookie để refresh: {e}")
        return False

    refresh_token = next(
        (c['value'] for c in cookies if c.get('name') == 'refresh_token'), None
    )
    if not refresh_token:
        logger.warning(
            "[Dailymotion] Cookie chưa có refresh_token — cần đăng nhập lại: "
            "python scripts/import_browser_cookies.py dailymotion"
        )
        return False

    try:
        import requests
        resp = requests.post(
            'https://graphql.api.dailymotion.com/oauth/token',
            data={
                'client_id': _DM_CLIENT_ID,
                'client_secret': _DM_CLIENT_SECRET,
                'grant_type': 'refresh_token',
                'refresh_token': refresh_token,
            },
            timeout=15,
        )
        if resp.status_code != 200:
            logger.warning(
                f"[Dailymotion] Refresh token thất bại HTTP {resp.status_code} "
                f"— refresh_token có thể đã hết hạn (~3 tháng) hoặc bị thu hồi. "
                f"Cần đăng nhập lại: python scripts/import_browser_cookies.py dailymotion. "
                f"Chi tiết: {resp.text[:150]}"
            )
            return False
        data = resp.json()
        new_access  = data.get('access_token')
        new_refresh = data.get('refresh_token')
        if not new_access or not new_refresh:
            logger.warning(f"[Dailymotion] Response refresh thiếu token: {str(data)[:150]}")
            return False
    except Exception as e:
        logger.warning(f"[Dailymotion] Refresh token lỗi mạng: {e}")
        return False

    expires_at = time.time() + data.get('expires_in', 36000) - 60
    updated = False
    for c in cookies:
        if c.get('name') == 'access_token':
            c['value'], c['expires'] = new_access, expires_at
            updated = True
        elif c.get('name') == 'refresh_token':
            c['value'] = new_refresh   # XOAY VÒNG — bắt buộc lưu lại
            updated = True

    if not updated:
        logger.warning("[Dailymotion] Không tìm thấy cookie access_token/refresh_token để ghi đè")
        return False

    try:
        with open(cookies_json_path, 'w') as f:
            _json.dump(cookies, f, indent=2)
        os.chmod(cookies_json_path, 0o600)
    except Exception as e:
        logger.warning(f"[Dailymotion] Không ghi được cookie đã refresh: {e}")
        return False

    logger.info(
        f"[Dailymotion] Đã tự làm mới access_token "
        f"(hết hạn sau {data.get('expires_in', 36000)//3600}h)"
    )
    return True


_DM_API_URL = 'https://api.dailymotion.com/videos'
_DM_API_PAGE_LIMIT = 100        # trần cứng của API cho mỗi request


def dailymotion_api_discover(
    keyword: str,
    limit: int = 100,
    max_duration_s: int | None = None,
    max_pages: int = 10,
    timeout: int = 25,
) -> list[str]:
    """
    Discovery Dailymotion qua API CÔNG KHAI — đường chính, không cần cookie.

    VÌ SAO KHÔNG QUÉT HTML NỮA (đo 2026-08-03):
        trang search HTML trần CỨNG 20 kết quả/từ khóa. ?page=1/2/3 trả về ĐÚNG
        CÙNG 20 ID (URL giữ tham số nhưng nội dung bỏ qua), không có link/nút
        phân trang nào, cuộn 12 lần không thêm 1 ID. Ghi chú cũ "mỗi trang 20,
        ?page=N để xem thêm" đã không còn đúng.

            HTML scrape : 20 URL / 8.5s   + cần cookie login (hay chết)
            API         : 500 URL / 6.5s  + không cần cookie

        Kết quả API còn ĐÚNG CHỦ ĐỀ HƠN: query 'cctv robbery' trả về
        "Robbery CCTV", "Robbery CCTV Footage", "Luton robbery CCTV"...

    `total` của API chốt ở 1000/query nên >10 request/từ khóa là vô nghĩa.
    Trang cuối hay lặp lại vài ID của trang trước — đã dedupe bằng `seen`.

    max_duration_s: lọc NGAY tại nguồn bằng tham số `shorter_than` (đơn vị
    PHÚT — đã kiểm chứng: shorter_than=15 → video dài nhất 6.5 phút,
    shorter_than=5 → dài nhất 3.0 phút). Tiết kiệm được cả lượt yt-dlp đọc
    metadata rồi mới bỏ ở match_filter.

    KHÔNG raise: lỗi mạng/HTTP → trả về những gì đã lấy được.
    """
    try:
        import requests
    except ImportError:
        logger.error('[Discovery] requests chưa được cài — không dùng được API Dailymotion')
        return []

    found: list[str] = []
    seen: set[str] = set()
    params = {
        'search': keyword,
        'sort': 'relevance',
        'fields': 'id',
        'limit': _DM_API_PAGE_LIMIT,
    }
    if max_duration_s and max_duration_s > 0:
        # Làm tròn LÊN để không tự loại oan video đúng bằng ngưỡng
        params['shorter_than'] = max(1, -(-max_duration_s // 60))

    for page in range(1, max_pages + 1):
        if len(found) >= limit:
            break
        try:
            resp = requests.get(
                _DM_API_URL, params={**params, 'page': page}, timeout=timeout
            )
            if resp.status_code != 200:
                logger.warning(
                    f"[Discovery] API Dailymotion HTTP {resp.status_code} "
                    f"(page={page}, '{keyword}'): {resp.text[:120]}"
                )
                break
            data = resp.json()
        except Exception as e:
            logger.warning(
                f"[Discovery] API Dailymotion lỗi (page={page}, '{keyword}'): {e}"
            )
            break

        for item in data.get('list') or []:
            vid = item.get('id')
            if vid and vid not in seen:
                seen.add(vid)
                found.append(f'https://www.dailymotion.com/video/{vid}')

        if not data.get('has_more'):
            break

    urls = found[:limit]
    logger.info(
        f"[Discovery] dailymotion API '{keyword}' → {len(urls)} URL "
        f"(tối đa {max_pages} request × {_DM_API_PAGE_LIMIT})"
    )
    return urls


_DM_NO_RESULT_MARKERS = (
    "we couldn't find",        # EN — "Well, you've got us there / We couldn't find…"
    "couldn't find anything",
    "aucun résultat",          # FR — Dailymotion là site Pháp, hay trả locale này
    "nous n'avons trouvé",
)


def _dm_no_real_results(page) -> bool:
    """
    Trang search Dailymotion có báo "không tìm thấy gì" hay không.

    Dailymotion KHÔNG trả lỗi hay trang trắng khi search 0 kết quả — nó hiện
    ~8 video gợi ý/trending kèm dòng chữ này. Không phát hiện được thì crawler
    tưởng đã tìm ra 8 video hợp lệ (xem mục 1.1 skill dailymotion-dag).

    Đã kiểm chứng 2026-07-31: cùng query, CÓ cookie và ẨN DANH trả về **cùng
    8 ID giống nhau** và cùng dòng chữ này ⇒ đây chính là chế độ fallback.

    Trả False khi không đọc được trang — thà để URL đi qua pipeline (dedup còn
    chặn được) hơn là chặn oan cả lượt discovery.
    """
    try:
        text = (page.inner_text('body') or '').lower()
    except Exception:
        return False
    return any(marker in text for marker in _DM_NO_RESULT_MARKERS)


def dailymotion_paginated_discover(
    keyword: str,
    limit: int = 40,
    cookies_json: str | None = None,
    max_pages: int | None = None,
    timeout_s: int | None = None,
) -> list[str]:
    """
    Discovery riêng cho Dailymotion — PHÂN TRANG qua ?page=N, không dùng cuộn.

    Đã kiểm chứng thực tế (2026-07-30): trang search của Dailymotion KHÔNG
    lazy-load thêm khi cuộn (scrollHeight không đổi dù cuộn hết), và không có
    nút "load more" — mỗi trang cố định 20 kết quả, muốn xem thêm phải đổi
    tham số URL ?page=N. Trang trùng kết quả với trang trước ⇒ đã hết dữ liệu.

    KHÔNG dùng filter `?duration=` của Dailymotion: đã kiểm chứng nó chỉ cho
    chọn 1 bucket/request (dù DOM dùng <input type=checkbox>, chọn bucket
    thứ 2 THAY THẾ bucket đầu, không cộng dồn, không có nút Apply để gộp
    nhiều lựa chọn). Loại video quá dài (>15 phút) được xử lý ở tầng khác:
    `platform_crawlers.config.max_duration_for('dailymotion')` (900s) truyền
    vào `GenericDownloader` — video dài hơn bị match_filter chặn TRƯỚC khi
    tải, không phải lọc ở bước search này.

    CẦN cookie đăng nhập (cookies_json) — khách ẩn danh chỉ nhận "no results"
    + gợi ý trending không liên quan, xem cookies/dailymotion_cookies.json
    (tạo bằng scripts/import_browser_cookies.py dailymotion).
    """
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as _FutTimeout

    from platform_crawlers import config as cfg

    max_pages = cfg.DAILYMOTION_MAX_PAGES if max_pages is None else max_pages
    timeout_s = cfg.DISCOVERY_TIMEOUT if timeout_s is None else timeout_s

    # Ngoài _run() để giữ được kết quả dở dang khi quá hạn (xem cuối hàm).
    seen: set[str] = set()
    found: list[str] = []
    stat = {'pages_fetched': 0}

    def _run() -> None:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            logger.error("[Discovery] playwright chưa được cài")
            return

        import json as _json
        cookies = []
        if cookies_json and os.path.exists(cookies_json):
            try:
                with open(cookies_json) as f:
                    cookies = _json.load(f)
            except Exception as e:
                logger.warning(f"[Discovery] Không đọc được cookie {cookies_json}: {e}")

        rx = re.compile(r'/video/[A-Za-z0-9]+')
        base = f'https://www.dailymotion.com/search/{quote(keyword)}/videos'

        try:
            with sync_playwright() as pw:
                browser = pw.chromium.launch(
                    headless=True,
                    args=['--disable-blink-features=AutomationControlled', '--no-sandbox'],
                )
                ctx = browser.new_context(
                    user_agent=(
                        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                        '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
                    ),
                    viewport={'width': 1280, 'height': 900},
                )
                ctx.add_init_script(
                    'Object.defineProperty(navigator,"webdriver",{get:()=>undefined})'
                )
                if cookies:
                    try:
                        ctx.add_cookies(cookies)
                    except Exception as e:
                        logger.warning(f"[Discovery] add_cookies lỗi: {e}")

                page = ctx.new_page()
                prev_page_ids: set[str] = set()

                for page_num in range(1, max_pages + 1):
                    if len(found) >= limit:
                        break
                    stat['pages_fetched'] = page_num

                    url = f'{base}?page={page_num}'
                    # ⚠ ĐỪNG dùng wait_until='networkidle' ở đây. Trang search
                    # Dailymotion KHÔNG BAO GIỜ đạt trạng thái networkidle (luôn
                    # còn request nền) nên goto() ném TimeoutError sau ĐÚNG 30s,
                    # mọi từ khóa, mọi lần — đã đo: networkidle 30.0s + ném lỗi,
                    # domcontentloaded 0.4s + không lỗi, CẢ HAI đọc ra cùng số
                    # link. Trước đây `except: break` biến lỗi chờ đó thành
                    # "0 URL (qua 1 trang)" cho toàn bộ lượt chạy.
                    try:
                        page.goto(url, wait_until='domcontentloaded', timeout=30_000)
                    except Exception as e:
                        # KHÔNG break: goto có thể ném vì điều kiện CHỜ không đạt
                        # dù trang đã tải và đọc được. Cứ thử đọc, không có link
                        # thì vòng dưới tự break.
                        logger.warning(
                            f"[Discovery] Dailymotion goto lỗi (page={page_num}): "
                            f"{type(e).__name__} — vẫn thử đọc trang"
                        )
                    # 3.5s: link video render nhanh (~2s) nhưng khối "không có kết
                    # quả" render CHẬM hơn — đọc sớm quá sẽ không thấy nó và tưởng
                    # là có kết quả thật (đã bị lừa 1 lần khi chỉ đợi 2.5s).
                    page.wait_for_timeout(3_500)

                    if page_num == 1 and _dm_no_real_results(page):
                        # Dailymotion KHÔNG báo lỗi khi search 0 kết quả — nó hiện
                        # ~8 video gợi ý/trending trông y như kết quả thật. Đẩy
                        # chúng vào pipeline là tự bắn vào chân: VLM loại chúng và
                        # ghi status 'skipped_not_cctv' — TRẠNG THÁI CUỐI, nên L0
                        # sẽ chặn vĩnh viễn dù sau này cookie đã sống lại.
                        logger.warning(
                            f"[Discovery] Dailymotion '{keyword}': trang báo KHÔNG "
                            f"CÓ KẾT QUẢ, các video đang hiện chỉ là gợi ý — bỏ qua "
                            f"để không ghi âm tính giả vào DB. Nguyên nhân thường "
                            f"gặp: cookie đăng nhập đã chết (access_token ~10-24h, "
                            f"refresh_token bị thu hồi). Chạy: "
                            f"python scripts/import_browser_cookies.py dailymotion"
                        )
                        break

                    this_page_ids: set[str] = set()
                    for a in page.query_selector_all('a[href]'):
                        href = a.get_attribute('href') or ''
                        m = rx.search(href)
                        if not m:
                            continue
                        # href của Dailymotion là đường dẫn TƯƠNG ĐỐI
                        # ('/video/x8ewysm'), có khi kèm slug/locale/query
                        # ('/fr/video/x8ewysm_ten-video?playlist=...').
                        # Dựng lại URL canonical từ ID để pipeline nhận URL
                        # tuyệt đối và dedup không tính 2 dạng là 2 video.
                        this_page_ids.add(f'https://www.dailymotion.com{m.group(0)}')

                    if not this_page_ids or this_page_ids == prev_page_ids:
                        # Trang rỗng hoặc trùng y hệt trang trước ⇒ đã hết
                        logger.debug(
                            f"[Discovery] Dailymotion hết trang tại page={page_num}"
                        )
                        break
                    prev_page_ids = this_page_ids

                    for full in this_page_ids:
                        if full not in seen:
                            seen.add(full)
                            found.append(full)

                browser.close()
        except Exception as e:
            logger.warning(f"[Discovery] Dailymotion phân trang lỗi: {e}")

        logger.info(
            f"[Discovery] dailymotion '{keyword}' → {len(found)} URL "
            f"(qua {stat['pages_fetched']} trang)"
        )

    # Không dùng `with ThreadPoolExecutor(...)` — xem giải thích ở
    # playwright_discover().
    ex = ThreadPoolExecutor(max_workers=1)
    try:
        ex.submit(_run).result(timeout=timeout_s)
    except _FutTimeout:
        logger.warning(
            f"[Discovery] Dailymotion quá {timeout_s}s ('{keyword}') — GIỮ LẠI "
            f"{len(found)} URL từ {stat['pages_fetched']} trang đầu"
        )
    except Exception as e:
        logger.warning(f"[Discovery] Dailymotion phân trang thread lỗi: {e}")
    finally:
        ex.shutdown(wait=False)

    return list(found)[:limit]


def discover_with_fallback(
    ytdlp_target: str,
    page_url: str,
    link_pattern: str,
    platform: str,
    limit: int = 40,
    cookies_file: str | None = None,
    cookies_json: str | None = None,
) -> list[str]:
    """
    Thử yt-dlp trước (nhanh, không cần browser), 0 kết quả thì quét HTML
    bằng Playwright.

    Cần thiết vì extractor của yt-dlp hay bị hỏng khi platform đổi API:
      - dailymotion:search  trả 0 entry KHÔNG kèm lỗi (đã kiểm tra 2026-07)
      - TikTok              báo "marked as broken / No working app info"
    Trang HTML công khai thì vẫn còn link, nên Playwright vá được.
    """
    urls = ytdlp_discover(
        ytdlp_target, limit=limit, platform=platform, cookies_file=cookies_file
    )
    if urls:
        return urls

    logger.info(
        f"[Discovery] yt-dlp không trả kết quả cho {platform} "
        f"→ thử Playwright: {page_url}"
    )
    return playwright_discover(
        page_url,
        link_pattern=link_pattern,
        limit=limit,
        cookies_json=cookies_json,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Reddit — public JSON API (không cần auth)
# ═══════════════════════════════════════════════════════════════════════════════

_REDDIT_UA = 'python:cctv-dataset-crawler:1.0 (by /u/researcher)'

# Token OAuth cache trong process (Reddit trả expires_in ~24h)
_reddit_token: dict = {'value': None, 'expires_at': 0.0}


def _reddit_oauth_token() -> str | None:
    """
    Lấy access token OAuth. Hai chế độ, tự chọn theo env đang có:

    1. USER LOGIN (grant_type=password) — khi có REDDIT_USERNAME + REDDIT_PASSWORD.
       Đăng nhập bằng chính tài khoản của bạn → THẤY ĐƯỢC BÀI NSFW.
       Quan trọng vì clip CCTV/crime trên Reddit rất thường bị gắn NSFW,
       và app-only auth thì KHÔNG thấy chúng.
       Yêu cầu: bật "I am over 18" trong cài đặt tài khoản Reddit.
       Tài khoản có 2FA: đặt REDDIT_PASSWORD="matkhau:123456" (mật khẩu:OTP).

    2. APP-ONLY (grant_type=client_credentials) — khi chỉ có CLIENT_ID/SECRET.
       Đọc được listing công khai nhưng bài NSFW bị ẩn.

    Cả hai đều cần tạo app trước:
      https://www.reddit.com/prefs/apps → "create app" → chọn type "script"
      → client_id nằm ngay dưới tên app, secret ở dòng "secret".
    """
    cid    = os.environ.get('REDDIT_CLIENT_ID', '').strip()
    secret = os.environ.get('REDDIT_CLIENT_SECRET', '').strip()
    if not cid or not secret:
        return None

    if _reddit_token['value'] and time.time() < _reddit_token['expires_at']:
        return _reddit_token['value']

    username = os.environ.get('REDDIT_USERNAME', '').strip()
    password = os.environ.get('REDDIT_PASSWORD', '').strip()

    if username and password:
        payload = {
            'grant_type': 'password',
            'username': username,
            'password': password,
        }
        mode = f'user login ({username}) — thấy được NSFW'
    else:
        payload = {'grant_type': 'client_credentials'}
        mode = 'app-only — KHÔNG thấy bài NSFW'

    import requests
    try:
        resp = requests.post(
            'https://www.reddit.com/api/v1/access_token',
            auth=(cid, secret),
            data=payload,
            headers={'User-Agent': _REDDIT_UA},
            timeout=20,
        )
        if resp.status_code != 200:
            logger.warning(
                f"[Discovery] Reddit OAuth thất bại HTTP {resp.status_code}: "
                f"{resp.text[:200]}"
            )
            return None
        data = resp.json()
        token = data.get('access_token')
        if not token:
            # Reddit trả 200 kèm {"error": "invalid_grant"} khi sai pass/thiếu OTP
            logger.warning(
                f"[Discovery] Reddit OAuth không có token: {str(data)[:200]}. "
                f"Nếu tài khoản bật 2FA, đặt REDDIT_PASSWORD=\"matkhau:OTP\"."
            )
            return None

        # Trừ 60s để không dùng token sát hạn
        _reddit_token['value'] = token
        _reddit_token['expires_at'] = time.time() + data.get('expires_in', 3600) - 60
        logger.info(f"[Discovery] Reddit OAuth OK — {mode}")
        return token
    except Exception as e:
        logger.warning(f"[Discovery] Reddit OAuth lỗi: {e}")
    return None


def reddit_discover(
    subreddit: str | None = None,
    query: str = '',
    limit: int = 40,
    timeout: int = 20,
) -> list[str]:
    """
    Lấy permalink các bài có VIDEO.

    subreddit=None + query  → search TOÀN Reddit (rộng nhất, 1 request/nhãn)
    subreddit + query       → search trong subreddit đó
    subreddit, query rỗng   → bài mới nhất của subreddit (không gán nhãn được)

    CÓ QUERY → đi tab "Media" của UI search (Playwright + cuộn), KHÔNG dùng
    API search. Lý do đã đo thực tế trên cùng query 'cctv robbery':

        tab Media + cuộn : 200 bài, 140 CÓ VIDEO (70%),  86s
        API search       : 224 bài,  41 có video (18%),  5.2s
        trùng nhau chỉ 46 bài — hai bên trả nội dung KHÁC HẲN nhau

    API search (cả .json lẫn oauth.reddit.com — cùng backend) nhanh hơn nhiều
    nhưng tìm được ÍT HƠN 3.4 LẦN số bài có video, nên không dùng cho query.
    Tab Media vẫn lẫn ảnh/link (60/200 bài), lọc lại bằng _reddit_keep_videos()
    — rẻ: 1 request/100 ID.

    KHÔNG có query (liệt kê r/<sub>/new) → vẫn dùng API, vì ở đây API trả
    thẳng listing đầy đủ metadata, không có tab Media nào để so.
    """
    where = f'r/{subreddit}' if subreddit else 'toàn Reddit'

    if query:
        return _reddit_discover_media_tab(subreddit, query, limit)

    token = _reddit_oauth_token()

    if not token:
        logger.warning(
            f"[Discovery] {where}: chưa có REDDIT_CLIENT_ID/SECRET "
            f"→ fallback Playwright (không lọc được video trước, chậm hơn). "
            f"Tạo app tại https://www.reddit.com/prefs/apps để dùng OAuth."
        )
        return _reddit_discover_playwright(subreddit, query, limit)

    import requests

    # include_over_18=on: cần thiết vì clip CCTV/crime hay bị gắn NSFW.
    # Chỉ có tác dụng khi token là user login (grant_type=password) và tài
    # khoản đã bật "I am over 18".
    n = min(limit, 100)
    if query and subreddit:
        url = (
            f'https://oauth.reddit.com/r/{subreddit}/search'
            f'?q={quote(query)}&restrict_sr=1&sort=new'
            f'&include_over_18=on&raw_json=1&limit={n}'
        )
    elif query:
        url = (
            f'https://oauth.reddit.com/search'
            f'?q={quote(query)}&sort=new'
            f'&include_over_18=on&raw_json=1&limit={n}'
        )
    else:
        url = f'https://oauth.reddit.com/r/{subreddit}/new?raw_json=1&limit={n}'

    try:
        resp = requests.get(
            url,
            headers={'Authorization': f'bearer {token}', 'User-Agent': _REDDIT_UA},
            timeout=timeout,
        )
        if resp.status_code != 200:
            logger.warning(f"[Discovery] Reddit {where} HTTP {resp.status_code}")
            return []
        children = resp.json().get('data', {}).get('children', [])
    except Exception as e:
        logger.warning(f"[Discovery] Reddit {where} lỗi: {e}")
        return []

    urls = []
    for child in children:
        d = child.get('data') or {}
        # Chỉ nhận bài thực sự có video (native v.redd.it hoặc embed video)
        has_video = (
            d.get('is_video')
            or d.get('post_hint') in ('hosted:video', 'rich:video')
            or d.get('domain') == 'v.redd.it'
        )
        if not has_video:
            continue
        permalink = d.get('permalink')
        if permalink:
            urls.append(f'https://www.reddit.com{permalink}')
        if len(urls) >= limit:
            break

    logger.info(f"[Discovery] {where} → {len(urls)} URL có video (OAuth)")
    return urls


# User-Agent PHẢI đầy đủ như Chrome thật. Đã kiểm chứng: 'Mozilla/5.0' ngắn
# bị Reddit trả 403 text/html trang "Blocked" NGAY CẢ KHI cookie còn hạn —
# rất dễ chẩn đoán nhầm thành "cookie hết hạn" rồi đi đăng nhập lại vô ích.
_REDDIT_BROWSER_UA = (
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
)

_REDDIT_POST_ID_RE = re.compile(r'/comments/([a-z0-9]+)')


def _reddit_cookies_dict() -> dict:
    """Cookie Netscape của Reddit → dict cho requests. {} nếu chưa có file."""
    from crawler_core.downloader import cookies_file_for

    path = cookies_file_for('reddit')
    if not path:
        return {}
    out = {}
    try:
        with open(path) as f:
            for line in f:
                if line.startswith('#') or not line.strip():
                    continue
                parts = line.rstrip('\n').split('\t')
                if len(parts) >= 7:
                    out[parts[5]] = parts[6]
    except Exception as e:
        logger.warning(f"[Discovery] Không đọc được cookie Reddit: {e}")
    return out


def _reddit_keep_videos(post_ids: list[str], timeout: int = 25) -> list[str]:
    """
    Lọc danh sách post ID, chỉ giữ bài THỰC SỰ CÓ VIDEO. Trả về URL đầy đủ
    (permalink thật lấy từ API — giữ được tên subreddit để đọc log/DB).

    Tab Media của Reddit gộp cả ảnh và link ngoài (đo thực tế: 60/200 bài
    không phải video — 36 ảnh, 16 link reddit.com, vài bài self). Không lọc
    thì rác đi thẳng vào pipeline, ăn hết quota max_per_label rồi bị ghi
    skipped_not_cctv (âm tính giả, L0 chặn vĩnh viễn).

    /api/info.json nhận tối đa 100 ID/request nên rẻ hơn nhiều so với mở
    từng bài. Cần cookie đăng nhập (ẩn danh bị 403).

    Không có cookie / request lỗi → trả nguyên danh sách, KHÔNG chặn luồng:
    thà để rác lọt vào pipeline còn hơn mất trắng cả lượt discovery.
    """
    if not post_ids:
        return []

    def _bare(pid: str) -> str:
        return f'https://www.reddit.com/comments/{pid}'

    cookies = _reddit_cookies_dict()
    if not cookies:
        logger.warning(
            "[Discovery] Chưa có cookie Reddit → bỏ qua bước lọc video, "
            "rác sẽ vào pipeline. Chạy: python scripts/import_browser_cookies.py reddit"
        )
        return [_bare(p) for p in post_ids]

    import requests

    kept: list[str] = []
    checked = 0
    for i in range(0, len(post_ids), 100):
        chunk = post_ids[i:i + 100]
        ids = ','.join(f't3_{pid}' for pid in chunk)
        try:
            resp = requests.get(
                f'https://www.reddit.com/api/info.json?id={ids}&raw_json=1',
                headers={'User-Agent': _REDDIT_BROWSER_UA},
                cookies=cookies,
                timeout=timeout,
            )
            if resp.status_code != 200:
                logger.warning(
                    f"[Discovery] info.json HTTP {resp.status_code} "
                    f"(ct={resp.headers.get('content-type', '')[:20]}) "
                    f"— giữ nguyên {len(chunk)} ID chưa lọc"
                )
                kept.extend(_bare(p) for p in chunk)
                continue
            children = resp.json().get('data', {}).get('children', [])
        except Exception as e:
            logger.warning(f"[Discovery] info.json lỗi: {e} — giữ nguyên {len(chunk)} ID")
            kept.extend(_bare(p) for p in chunk)
            continue

        checked += len(chunk)
        for child in children:
            d = child.get('data') or {}
            has_video = (
                d.get('is_video')
                or d.get('post_hint') in ('hosted:video', 'rich:video')
                or d.get('domain') == 'v.redd.it'
            )
            if not has_video:
                continue
            permalink = d.get('permalink')
            if permalink:
                kept.append(f'https://www.reddit.com{permalink}')
            elif d.get('id'):
                kept.append(_bare(d['id']))

    if checked:
        logger.info(
            f"[Discovery] Lọc video: {len(kept)}/{checked} bài có video "
            f"({checked - len(kept)} bài ảnh/text bị loại trước khi vào pipeline)"
        )
    return kept


def _reddit_discover_media_tab(
    subreddit: str | None, query: str, limit: int
) -> list[str]:
    """
    Search rồi vào TAB MEDIA của UI, cuộn để tải thêm — đường cho kết quả
    video tốt nhất (xem giải thích số liệu ở reddit_discover()).

    Tham số URL tương ứng các nút trên UI:
        type=media        tab "Phương tiện"  (chỉ UI hiểu; API search BỎ QUA)
        sort=relevance    "Mức độ phù hợp"
        t=all             "Tất cả thời gian"
        include_over_18=on  "Tắt tìm kiếm an toàn" — chỉ có tác dụng khi
                          cookie là tài khoản đã bật "I am over 18"

    Cuộn chuột = playwright_discover() tự làm (mouse.wheel, dừng khi 3 lần
    cuộn liên tiếp không ra link mới).
    """
    from crawler_core.downloader import cookies_json_for

    q = quote(query)
    common = f'q={q}&type=media&sort=relevance&t=all&include_over_18=on'
    if subreddit:
        page = f'https://www.reddit.com/r/{subreddit}/search/?{common}&restrict_sr=1'
        where = f'r/{subreddit}'
    else:
        page = f'https://www.reddit.com/search/?{common}'
        where = 'toàn Reddit'

    # Lấy dư rồi mới lọc video: tab Media lẫn ~30% ảnh/link, xin đúng `limit`
    # sẽ hụt sau khi lọc.
    urls = playwright_discover(
        page,
        link_pattern=r'/comments/[a-z0-9]+',
        limit=limit * 3,
        cookies_json=cookies_json_for('reddit'),
    )

    # Dựng lại URL canonical từ ID: href trên trang có cả dạng
    # /r/<sub>/comments/<id>/<slug>/ và /comments/<id>/ → cùng 1 bài nhưng
    # khác chuỗi, dedup L0 (so khớp chuỗi URL) sẽ tính là 2 bài khác nhau.
    ids: list[str] = []
    seen: set[str] = set()
    for u in urls:
        m = _REDDIT_POST_ID_RE.search(u)
        if m and m.group(1) not in seen:
            seen.add(m.group(1))
            ids.append(m.group(1))

    video_urls = _reddit_keep_videos(ids)[:limit]
    logger.info(
        f"[Discovery] {where} (tab Media) → {len(video_urls)} URL có video "
        f"(từ {len(ids)} bài tìm được)"
    )
    return video_urls


def _reddit_discover_playwright(
    subreddit: str | None, query: str, limit: int
) -> list[str]:
    """
    Fallback khi không có OAuth credentials — quét HTML công khai.
    Nếu đã lưu cookie đăng nhập (scripts/save_platform_cookies.py reddit)
    thì dùng luôn để thấy được bài NSFW.
    """
    from crawler_core.downloader import cookies_json_for

    if query and subreddit:
        page = (f'https://www.reddit.com/r/{subreddit}/search/'
                f'?q={quote(query)}&restrict_sr=1&sort=new')
    elif query:
        page = f'https://www.reddit.com/search/?q={quote(query)}&sort=new'
    else:
        page = f'https://www.reddit.com/r/{subreddit}/new/'

    return playwright_discover(
        page,
        link_pattern=r'/comments/[a-z0-9]+',
        limit=limit,
        cookies_json=cookies_json_for('reddit'),
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Facebook — search tab "Thước phim" (/search/videos/), cuộn để tải thêm
# ═══════════════════════════════════════════════════════════════════════════════

# ID video Facebook nằm ở 3 dạng href khác nhau trên trang kết quả:
#   /watch/?ref=search&v=1582385296612845&...   ← dạng CHÍNH của tab Thước phim
#   /tu.quy.536275/videos/2498077680477253/
#   /reel/1234567890
# Đã đếm thực tế trên 1 query: 598 lượt khớp dạng ?v= so với 18 lượt /videos/.
_FB_ID_PATTERNS = [
    (re.compile(r'[?&]v=(\d+)'),      'video'),
    (re.compile(r'/videos/(\d+)'),    'video'),
    (re.compile(r'/reel/(\d+)'),      'reel'),
]


def facebook_search_discover(
    query: str,
    limit: int = 40,
    cookies_json: str | None = None,
    max_scrolls: int | None = None,
    headless: bool = True,
    timeout_s: int | None = None,
) -> list[str]:
    """
    Tìm video trên Facebook qua tab "Thước phim" của trang search.

    KHÔNG dùng lại playwright_discover() được — hàm đó có `full.split('?')[0]`
    cắt bỏ query string, mà ID video Facebook lại NẰM TRONG query
    (`/watch/?v=<id>`). Dùng chung sẽ ra toàn URL `https://facebook.com/watch/`
    trống rỗng, mất hết ID.

    Tab "Thước phim" trên UI tiếng Việt chính là `/search/videos/` — đã kiểm
    chứng bằng cách đọc href của các tab: Tất cả→/search/top/,
    Mọi người→/search/people/, **Thước phim→/search/videos/**, Trang→/search/pages/.

    Cuộn có tác dụng rõ rệt, đo thực tế trên query 'cctv cướp giật':
        1 lần cuộn → 14 ID | 4 → 35 | 8 → 63 | 12 → 91 ID unique
    (vẫn tăng đều, chưa chạm đáy — tăng max_scrolls sẽ ra thêm.)

    ⚠ Như playwright_discover: `limit` (CRAWL_MAX_PER_TARGET) mới là thứ chặn
    vòng cuộn trong thực tế, không phải max_scrolls.

    Trả về URL canonical để dedup L1 nhận đúng loại:
        video → https://www.facebook.com/watch/?v=<id>
        reel  → https://www.facebook.com/reel/<id>
    """
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as _FutTimeout

    from platform_crawlers import config as cfg

    max_scrolls = cfg.FACEBOOK_MAX_SCROLLS if max_scrolls is None else max_scrolls
    timeout_s   = cfg.DISCOVERY_TIMEOUT if timeout_s is None else timeout_s

    # Ngoài _run() để giữ được kết quả dở dang khi quá hạn (xem cuối hàm).
    found: dict[str, str] = {}          # id -> url canonical

    def _run() -> None:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            logger.error("[Discovery] playwright chưa được cài")
            return

        import json as _json
        cookies = []
        if cookies_json and os.path.exists(cookies_json):
            try:
                with open(cookies_json) as f:
                    cookies = _json.load(f)
            except Exception as e:
                logger.warning(f"[Discovery] Không đọc được cookie FB {cookies_json}: {e}")

        page_url = f'https://www.facebook.com/search/videos/?q={quote(query)}'

        try:
            with sync_playwright() as pw:
                browser = pw.chromium.launch(
                    headless=headless,
                    args=['--disable-blink-features=AutomationControlled', '--no-sandbox'],
                )
                ctx = browser.new_context(
                    user_agent=(
                        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                        '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
                    ),
                    viewport={'width': 1360, 'height': 950},
                    # locale vi-VN để nhãn tab khớp với tài liệu ("Thước phim");
                    # không ảnh hưởng kết quả nhưng dễ debug khi chụp màn hình.
                    locale='vi-VN',
                )
                ctx.add_init_script(
                    'Object.defineProperty(navigator,"webdriver",{get:()=>undefined})'
                )
                if cookies:
                    try:
                        ctx.add_cookies(cookies)
                    except Exception as e:
                        logger.warning(f"[Discovery] add_cookies FB lỗi: {e}")

                page = ctx.new_page()
                page.goto(page_url, wait_until='domcontentloaded', timeout=60_000)
                page.wait_for_timeout(8_000)

                # Guard này bắt trường hợp FB chuyển hướng sang login/checkpoint.
                # LƯU Ý: khi THIẾU HẲN cookie thì không tới được đây — page.goto()
                # ném ERR_CONNECTION_REFUSED và rơi vào except bên dưới.
                if 'login' in page.url or 'checkpoint' in page.url:
                    logger.warning(
                        f"[Discovery] Facebook đá về đăng nhập ({page.url[:80]}) — "
                        f"cookie hỏng/hết hạn. Chạy: "
                        f"python scripts/import_browser_cookies.py facebook"
                    )
                    browser.close()
                    return

                empty_streak = 0
                for _ in range(max_scrolls):
                    before = len(found)
                    for a in page.query_selector_all('a[href]'):
                        href = a.get_attribute('href') or ''
                        for rx, kind in _FB_ID_PATTERNS:
                            m = rx.search(href)
                            if not m:
                                continue
                            vid = m.group(1)
                            if vid in found:
                                break
                            found[vid] = (
                                f'https://www.facebook.com/reel/{vid}' if kind == 'reel'
                                else f'https://www.facebook.com/watch/?v={vid}'
                            )
                            break

                    if len(found) >= limit:
                        break

                    # 3 vòng cuộn liên tiếp không ra ID mới → coi như hết
                    empty_streak = empty_streak + 1 if len(found) == before else 0
                    if empty_streak >= 3:
                        break

                    try:
                        page.mouse.wheel(0, 3500)
                    except Exception:
                        break
                    page.wait_for_timeout(2_500)

                browser.close()
        except Exception as e:
            logger.warning(f"[Discovery] Facebook Playwright lỗi ({query}): {e}")

        logger.info(f"[Discovery] FB Thước phim '{query}' → {len(found)} video")

    # Không dùng `with ThreadPoolExecutor(...)` — xem giải thích ở
    # playwright_discover(): __exit__ chờ thread xong bất chấp timeout và làm
    # mất trắng kết quả đã gom.
    ex = ThreadPoolExecutor(max_workers=1)
    try:
        ex.submit(_run).result(timeout=timeout_s)
    except _FutTimeout:
        logger.warning(
            f"[Discovery] FB quá {timeout_s}s ('{query}') — GIỮ LẠI "
            f"{len(found)} video đã gom được"
        )
    except Exception as e:
        logger.warning(f"[Discovery] Facebook thread lỗi: {e}")
    finally:
        ex.shutdown(wait=False)

    return list(found.values())[:limit]


# ═══════════════════════════════════════════════════════════════════════════════
# Playwright — cho platform không có extractor liệt kê (X/Twitter)
# ═══════════════════════════════════════════════════════════════════════════════

def playwright_discover(
    page_url: str,
    link_pattern: str,
    limit: int = 40,
    cookies_json: str | None = None,
    max_scrolls: int | None = None,
    headless: bool = True,
    timeout_s: int | None = None,
) -> list[str]:
    """
    Mở 1 trang, CUỘN TỚI ĐÁY và gom mọi href khớp `link_pattern`.

    Vòng cuộn dừng ở điều kiện nào đến trước:
        len(found) >= limit          ← thường là cái này, xem cảnh báo dưới
        3 vòng liên tiếp không ra link mới  = đã tới đáy thật
        hết max_scrolls
        hết timeout_s

    ⚠ MUỐN CUỘN SÂU HƠN THÌ PHẢI TĂNG `limit` TRƯỚC. Tăng max_scrolls một mình
    không có tác dụng gì, vì `if len(found) >= limit: break` gần như luôn chạm
    trước. limit đến từ CRAWL_MAX_PER_TARGET.

    Chạy trong thread riêng: Playwright sync API xung đột với event loop
    của Airflow nếu gọi trực tiếp trong task.
    """
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as _FutTimeout

    from platform_crawlers import config as cfg

    max_scrolls = cfg.MAX_SCROLLS if max_scrolls is None else max_scrolls
    timeout_s   = cfg.DISCOVERY_TIMEOUT if timeout_s is None else timeout_s

    # Accumulator nằm NGOÀI _run() để lấy được kết quả DỞ DANG khi quá hạn —
    # xem khối try/finally ở cuối hàm.
    found: list[str] = []
    seen: set[str] = set()

    def _run() -> None:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            logger.error("[Discovery] playwright chưa được cài")
            return

        import json as _json
        cookies = []
        if cookies_json and os.path.exists(cookies_json):
            try:
                with open(cookies_json) as f:
                    cookies = _json.load(f)
            except Exception as e:
                logger.warning(f"[Discovery] Không đọc được cookie {cookies_json}: {e}")

        rx = re.compile(link_pattern)

        try:
            with sync_playwright() as pw:
                browser = pw.chromium.launch(
                    headless=headless,
                    args=['--disable-blink-features=AutomationControlled', '--no-sandbox'],
                )
                ctx = browser.new_context(
                    user_agent=(
                        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                        '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
                    ),
                    viewport={'width': 1280, 'height': 900},
                )
                ctx.add_init_script(
                    'Object.defineProperty(navigator,"webdriver",{get:()=>undefined})'
                )
                if cookies:
                    try:
                        ctx.add_cookies(cookies)
                    except Exception as e:
                        logger.warning(f"[Discovery] add_cookies lỗi: {e}")

                page = ctx.new_page()
                page.goto(page_url, wait_until='domcontentloaded', timeout=45_000)
                page.wait_for_timeout(4_000)

                empty_streak = 0
                for _ in range(max_scrolls):
                    before = len(seen)
                    for a in page.query_selector_all('a[href]'):
                        href = a.get_attribute('href') or ''
                        if not rx.search(href):
                            continue
                        full = href if href.startswith('http') else _origin(page_url) + href
                        full = full.split('?')[0]
                        if full not in seen:
                            seen.add(full)
                            found.append(full)

                    if len(found) >= limit:
                        break

                    # 3 lần scroll không thêm link mới → coi như hết nội dung
                    empty_streak = empty_streak + 1 if len(seen) == before else 0
                    if empty_streak >= 3:
                        break

                    try:
                        # mouse.wheel() giả lập cuộn chuột THẬT — đã kiểm chứng
                        # thực tế: window.scrollBy() không di chuyển được trang
                        # trên Dailymotion (scrollY luôn = 0 dù gọi scrollBy),
                        # trong khi mouse.wheel()/scrollTo() hoạt động đúng.
                        page.mouse.wheel(0, 3000)
                    except Exception:
                        break
                    page.wait_for_timeout(2_500)

                browser.close()
        except Exception as e:
            logger.warning(f"[Discovery] Playwright lỗi ({page_url}): {e}")

        logger.info(f"[Discovery] {page_url} → {len(found)} URL")

    # ⚠ KHÔNG dùng `with ThreadPoolExecutor(...) as ex` ở đây. __exit__ gọi
    # shutdown(wait=True) nên nó CHỜ thread chạy xong bất chấp timeout — đã đo:
    # timeout=1s nhưng thực tế chờ 3s với hàm sleep(3). Cách cũ vừa không giới
    # hạn được thời gian thật, vừa `return []` làm MẤT TRẮNG hàng trăm URL đã
    # gom được. Cuộn sâu (limit lớn) chạm ngưỡng này rất dễ.
    ex = ThreadPoolExecutor(max_workers=1)
    try:
        ex.submit(_run).result(timeout=timeout_s)
    except _FutTimeout:
        logger.warning(
            f"[Discovery] Quá {timeout_s}s ({page_url}) — GIỮ LẠI {len(found)} URL "
            f"đã gom được (tăng CRAWL_DISCOVERY_TIMEOUT nếu muốn cuộn sâu hơn)"
        )
    except Exception as e:
        logger.warning(f"[Discovery] Playwright thread lỗi: {e}")
    finally:
        # wait=False: không chặn. Thread nền tự đóng browser khi ra khỏi
        # `with sync_playwright()`, nên Chromium chỉ sống thêm một lúc rồi tự dọn.
        ex.shutdown(wait=False)

    return list(found)[:limit]


def _origin(url: str) -> str:
    from urllib.parse import urlparse
    p = urlparse(url)
    return f'{p.scheme}://{p.netloc}'
