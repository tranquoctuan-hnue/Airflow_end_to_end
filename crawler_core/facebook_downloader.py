import os
import re
import json
import logging
import shutil
import subprocess
import tempfile
import threading
import requests
import yt_dlp

logger = logging.getLogger(__name__)

BASE_DIR      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Chỗ lưu video dùng CHUNG với GenericDownloader (env CRAWL_VIDEO_OUTPUT_DIR) — một nguồn
# sự thật duy nhất. Trước đây file này có bản sao riêng trỏ vào data/videos, nên đổi chỗ
# lưu ở crawler_core mà quên đây thì video Facebook vẫn rơi vào ổ nội bộ — lỗi âm thầm,
# chỉ phát hiện khi đi tìm file.
from crawler_core.downloader import (  # noqa: E402
    OUTPUT_BASE, ensure_output_base, ensure_staging_dir as _staging_dir,
)

# Mặc định = cookie của DAG Facebook nhóm (cũ). DAG social_crawler_fb truyền
# cookies_file=cookies/facebook_cookies.txt riêng — xem social_crawler_common.py.
COOKIES_FILE  = os.path.join(BASE_DIR, 'cookies', 'facebook_legacy_cookies.txt')
COOKIES_JSON  = os.path.join(BASE_DIR, 'cookies', 'facebook_legacy_cookies.json')

FB_CDN_PATTERN = 'fbcdn.net'


class VideoDownloader:
    def __init__(self, output_base: str = OUTPUT_BASE, cookies_file: str = COOKIES_FILE):
        self.output_base  = output_base
        self.cookies_file = cookies_file
        ensure_output_base(output_base)
        logger.info(f"[FB Download] Lưu video vào: {output_base}")

    # ── Public ──────────────────────────────────────────────────────────

    def download(self, url: str, category: str) -> str | None:
        """
        Tải video vào <OUTPUT_BASE>/{category}/ (mặc định là NAS pvn_share).
        Thử yt-dlp trước, hỏng thì bắt từng DASH segment bằng Playwright.
        Trả đường dẫn file khi thành công, None khi thất bại.
        Mọi file trả về đều được kiểm tra có video stream playable hay không.

        TẢI VÀO Ổ NỘI BỘ TRƯỚC rồi mới chuyển sang đích — bắt buộc khi đích là ổ
        mạng. Cả hai đường tải ở đây đều ghép file từ NHIỀU MẢNH (yt-dlp dùng
        .part/.ytdl, Playwright ghép DASH segment bằng ffmpeg) mà gvfs/SMB không
        chịu được: đã tái hiện `[Errno 2] ...mp4.ytdl` và file ghép ra hỏng.
        Xem crawler_core.downloader.GenericDownloader.download để biết chi tiết.
        """
        category_dir = os.path.join(self.output_base, category)
        os.makedirs(category_dir, exist_ok=True)

        result = self.fetch(url)
        if not result:
            return None
        dest = os.path.join(category_dir, os.path.basename(result))
        try:
            # copy_function=copyfile: gvfs không hỗ trợ chmod/utime nên copy2
            # (mặc định của move) sẽ ném OSError [Errno 95].
            shutil.move(result, dest, copy_function=shutil.copyfile)
            return dest
        except Exception as e:
            logger.error(
                f"[FB Download] Không chuyển được file sang {category_dir}: "
                f"{type(e).__name__}: {e}"
            )
            return None
        finally:
            shutil.rmtree(os.path.dirname(result), ignore_errors=True)

    def fetch(self, url: str) -> str | None:
        """
        Tải video vào 1 thư mục TẠM riêng dưới staging, KHÔNG chuyển đi đâu.
        Cùng hợp đồng với crawler_core.downloader.GenericDownloader.fetch:
        caller tự chuyển file rồi gọi release_staged(path) để dọn thư mục tạm.
        """
        staging = tempfile.mkdtemp(prefix='fbdl_', dir=_staging_dir())
        result = self._download_ytdlp(url, staging)
        if not result:
            logger.info(f"yt-dlp thất bại, thử Playwright: {url}")
            result = self._download_playwright(url, staging)

        if not result:
            shutil.rmtree(staging, ignore_errors=True)
            return None

        # Kiểm tra cuối: file phải có video stream đọc được.
        if not self._has_video_stream(result):
            logger.warning(f"[Validate] File không có video stream playable — bỏ: {url}")
            shutil.rmtree(staging, ignore_errors=True)
            return None
        return result

    @staticmethod
    def _has_video_stream(file_path: str) -> bool:
        """True nếu ffprobe đọc được ít nhất 1 video stream từ file."""
        try:
            r = subprocess.run(
                ['ffprobe', '-v', 'error',
                 '-select_streams', 'v:0',
                 '-show_entries', 'stream=codec_type',
                 '-of', 'csv=p=0', file_path],
                capture_output=True, timeout=30,
            )
            return r.returncode == 0 and b'video' in r.stdout
        except Exception:
            return False

    # ── yt-dlp ──────────────────────────────────────────────────────────

    def _download_ytdlp(self, url: str, category_dir: str) -> str | None:
        ydl_opts = {
            'outtmpl': os.path.join(category_dir, '%(id)s.%(ext)s'),
            # Ưu tiên format đã có sẵn cả video+audio trong 1 file (tránh fMP4 video-only
            # của Facebook vì loại này thiếu moov atom → ffprobe báo "moov atom not found").
            # Fallback cuối mới dùng bestvideo+bestaudio (cần ffmpeg merge).
            'format': (
                'best[ext=mp4][vcodec!=none][acodec!=none]'
                '/best[ext=mp4]'
                '/bestvideo[ext=mp4]+bestaudio[ext=m4a]'
                '/best'
            ),
            'merge_output_format': 'mp4',
            # fixup=detect_or_warn: tự sửa fMP4 fragment sau khi tải xong nếu có thể
            'fixup': 'detect_or_warn',
            'quiet': True,
            'no_warnings': True,
            'retries': 3,
            'fragment_retries': 5,
            'socket_timeout': 30,
        }
        if os.path.exists(self.cookies_file):
            ydl_opts['cookiefile'] = self.cookies_file

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=True)
                if not info:
                    return None
                filename = ydl.prepare_filename(info)
                for ext in ['.mp4', '.mkv', '.webm', '']:
                    candidate = os.path.splitext(filename)[0] + ext
                    if os.path.exists(candidate):
                        logger.info(f"[yt-dlp] Downloaded → {os.path.basename(candidate)}")
                        return candidate
        except Exception as e:
            logger.debug(f"[yt-dlp] {e}")
        return None

    # ── Playwright DASH segment collector ───────────────────────────────

    def _load_playwright_cookies(self) -> list:
        if os.path.exists(COOKIES_JSON):
            try:
                with open(COOKIES_JSON) as f:
                    return json.load(f)
            except Exception:
                pass
        return []

    def _download_playwright(self, url: str, category_dir: str) -> str | None:
        from concurrent.futures import ThreadPoolExecutor
        try:
            with ThreadPoolExecutor(max_workers=1) as ex:
                return ex.submit(self._playwright_collect, url, category_dir).result(timeout=600)
        except Exception as e:
            logger.error(f"[Playwright] Thread thất bại: {e}")
            return None

    def _playwright_collect(self, url: str, category_dir: str) -> str | None:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            logger.error("playwright không được cài")
            return None

        # Store (seg_url, data) to separate streams later
        segs = []
        lock = threading.Lock()

        def _on_response(response):
            if FB_CDN_PATTERN not in response.url:
                return
            ct = response.headers.get('content-type', '')
            url = response.url
            # Facebook DASH: video/mp4, audio/mp4 — đôi khi init segment dùng
            # application/octet-stream → cũng capture nếu URL có bytestart/byteend
            is_media_ct  = 'video' in ct or 'audio' in ct
            is_dash_url  = 'bytestart' in url or 'byteend' in url or '/dash/' in url
            if not is_media_ct and not (is_dash_url and 'fbcdn.net' in url):
                return
            try:
                data = response.body()
                if len(data) < 10:      # bỏ qua response rỗng/error nhỏ
                    return
                with lock:
                    segs.append((url, data))
                    logger.debug(f"[Playwright] seg {len(segs)}: {len(data)//1024}KB ct={ct[:20]}"
                    )
            except Exception:
                pass

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
                    viewport={'width': 1280, 'height': 800},
                )
                ctx.add_init_script(
                    'Object.defineProperty(navigator,"webdriver",{get:()=>undefined})'
                )

                cookies = self._load_playwright_cookies()
                if cookies:
                    ctx.add_cookies(cookies)

                page = ctx.new_page()
                page.on('response', _on_response)

                page.goto(url, wait_until='domcontentloaded', timeout=30_000)
                page.wait_for_timeout(3_000)

                # Phát hiện sớm video đã bị gỡ / không khả dụng → bỏ qua ngay
                try:
                    body_text = page.evaluate('() => document.body.innerText || ""')
                except Exception:
                    body_text = ''
                REMOVED_MARKERS = (
                    'không còn nữa', 'đã bị gỡ', 'liên kết bị hỏng',
                    "isn't available", 'no longer available',
                    'content isn', 'video unavailable',
                )
                if any(m in body_text for m in REMOVED_MARKERS):
                    logger.warning(f"[Playwright] Video đã bị gỡ/không khả dụng: {url}")
                    browser.close()
                    return None

                # Bypass sensitive content overlay:
                # Facebook hiển thị overlay blur + nút "Tìm hiểu thêm" (KHÔNG phải "Xem nội dung").
                # Cách đúng: click vào vùng video bị che, sau đó JS force play để bypass hoàn toàn.
                try:
                    video_el = page.query_selector('video')
                    if video_el:
                        box = video_el.bounding_box()
                        if box:
                            cx = box['x'] + box['width'] / 2
                            cy = box['y'] + box['height'] / 2
                            page.mouse.click(cx, cy)
                            logger.info("[Playwright] Click vào vùng video overlay")
                            page.wait_for_timeout(1_000)
                except Exception:
                    pass

                # JS force play ngay sau khi load — bypass mọi overlay UI
                try:
                    page.evaluate(
                        '() => { const v = document.querySelector("video"); '
                        'if (v) { v.muted = true; v.play().catch(() => {}); } }'
                    )
                    page.wait_for_timeout(1_000)
                except Exception:
                    pass

                # Chờ <video> element xuất hiện (FB lazy-load)
                try:
                    page.wait_for_selector('video', timeout=15_000)
                except Exception:
                    logger.debug("[Playwright] Không thấy <video> element")

                def _force_play():
                    """Click + force play bằng JS, lặp lại nhiều selector."""
                    for sel in ('video', '[aria-label="Play"]', '[aria-label="Phát"]',
                                'div[role="button"]'):
                        try:
                            page.click(sel, timeout=2_000)
                            break
                        except Exception:
                            continue
                    try:
                        page.evaluate(
                            '() => { const v = document.querySelector("video"); '
                            'if (v) { v.muted = true; v.play().catch(()=>{}); } }'
                        )
                    except Exception:
                        pass

                # Trigger play, đợi readyState; nếu chưa có segment nào thì
                # thử play lại tối đa 3 lần trước khi bỏ cuộc
                for attempt in range(3):
                    _force_play()
                    try:
                        page.wait_for_function(
                            '() => { const v = document.querySelector("video"); '
                            'return v && v.readyState >= 3; }',
                            timeout=12_000,
                        )
                    except Exception:
                        pass
                    page.wait_for_timeout(1_500)
                    with lock:
                        n_segs = len(segs)
                    if n_segs > 0:
                        break
                    logger.debug(
                        f"[Playwright] Chưa có segment (lần {attempt + 1}/3) — thử play lại"
                    )

                # Lấy duration để seek qua toàn bộ video
                try:
                    duration = page.evaluate(
                        '() => { const v = document.querySelector("video"); '
                        'return (v && isFinite(v.duration) && v.duration > 0) '
                        '       ? v.duration : 0; }'
                    )
                except Exception:
                    duration = 0

                def _seek(t: float):
                    try:
                        page.evaluate(
                            f'() => {{ const v = document.querySelector("video"); '
                            f'if (v) v.currentTime = {t:.2f}; }}'
                        )
                        page.wait_for_timeout(1_000)
                    except Exception:
                        pass

                if duration > 0:
                    logger.debug(f"[Playwright] Duration={duration:.1f}s — seeking qua video")
                    # Step 0.5s: đủ nhỏ để capture audio segment ngắn (~0.4-1.2s/seg)
                    # Tối đa 300 bước; với video dài step tự tăng để giữ trong budget
                    step = max(0.5, duration / 300)
                    t = 0.0
                    while t < duration:
                        _seek(t)
                        t += step
                    _seek(max(0, duration - 1))   # segment cuối
                else:
                    # Duration không lấy được → seek qua các mốc cố định
                    # để kéo segments thay vì chỉ chờ
                    logger.debug("[Playwright] Duration=0 — seeking blind qua mốc cố định")
                    for t in [0, 5, 10, 20, 30, 60, 90, 120, 180, 240, 300]:
                        _seek(t)

                page.wait_for_timeout(2_000)  # trailing segments
                browser.close()

        except Exception as e:
            logger.error(f"[Playwright] Browser lỗi: {e}")
            return None

        if not segs:
            logger.warning(f"[Playwright] Không có segment nào: {url}")
            return None

        return self._mux_dash_segments(segs, category_dir, url)

    # ── DASH stream separation & mux ────────────────────────────────────

    @staticmethod
    def _detect_handler(data: bytes) -> str | None:
        """
        Đọc handler_type từ fMP4 hdlr box. Duyệt TẤT CẢ hdlr occurrence,
        trả về 'vide' hoặc 'soun' khi tìm thấy. Bỏ qua metadata handlers
        như 'ID32', 'text', 'url ', v.v.
        Box layout: ...[hdlr(4)][version+flags(4)][pre_defined(4)][handler_type(4)]...
        → handler_type tại offset +12 từ vị trí 'h' của 'hdlr'.
        """
        pos = 0
        while True:
            idx = data.find(b'hdlr', pos)
            if idx < 0 or idx + 16 > len(data):
                break
            handler = data[idx + 12:idx + 16]
            if handler in (b'vide', b'soun'):
                return handler.decode('ascii')
            pos = idx + 4
        return None

    @staticmethod
    def _stream_key(seg_url: str) -> str:
        """Group segments by stream: same CDN path = same DASH stream."""
        from urllib.parse import urlparse
        p = urlparse(seg_url)
        return p.netloc + p.path

    @staticmethod
    def _seg_dedup_key(seg_url: str) -> str:
        """
        Dedup key dựa trên path + bytestart + byteend, bỏ qua signed token
        (oe=, _nc_ht=...) có thể thay đổi giữa các request trong cùng session.
        """
        from urllib.parse import urlparse, parse_qs
        p = urlparse(seg_url)
        qs = parse_qs(p.query)
        bs = qs.get('bytestart', ['0'])[0]
        be = qs.get('byteend', [''])[0]
        return f"{p.netloc}{p.path}:{bs}-{be}"

    @staticmethod
    def _seg_bytestart(seg_url: str) -> int:
        """Trả về bytestart để sắp xếp đúng thứ tự trong stream."""
        from urllib.parse import urlparse, parse_qs
        qs = parse_qs(urlparse(seg_url).query)
        try:
            return int(qs.get('bytestart', ['0'])[0])
        except Exception:
            return 0

    @staticmethod
    def _video_id_from_url(source_url: str) -> str:
        m = (
            re.search(r'/videos/(\d+)', source_url)
            or re.search(r'[?&]v=(\d+)', source_url)
            or re.search(r'/reel/(\d+)', source_url)
            or re.search(r'[/=](\d{8,})', source_url)
        )
        return m.group(1) if m else 'video'

    def _mux_dash_segments(
        self, segs: list, category_dir: str, source_url: str
    ) -> str | None:
        """
        Tách các segment thành video stream và audio stream dựa trên URL path,
        phân loại bằng fMP4 hdlr box, rồi mux bằng ffmpeg.
        """
        from collections import defaultdict

        # Dedup theo (path + bytestart + byteend): loại bỏ cùng data dù signed token khác
        seen_keys: set[str] = set()
        unique_segs: list[tuple[str, int, bytes]] = []
        for seg_url, data in segs:
            dk = self._seg_dedup_key(seg_url)
            if dk not in seen_keys:
                seen_keys.add(dk)
                unique_segs.append((seg_url, self._seg_bytestart(seg_url), data))

        # Group by stream URL path — lưu cả URL để fetch init segment nếu cần
        streams: dict[str, list[tuple[str, int, bytes]]] = defaultdict(list)
        for seg_url, bytestart, data in unique_segs:
            streams[self._stream_key(seg_url)].append((seg_url, bytestart, data))

        # Init segment: ưu tiên ftyp/moov ở đầu data, sau đó sắp theo bytestart
        def _init_first(item):
            _, bytestart, data = item
            is_init = (len(data) >= 8 and data[4:8] == b'ftyp') or b'moov' in data[:4096]
            return (0 if is_init else 1, bytestart)

        for key in streams:
            streams[key].sort(key=_init_first)

        # Phân loại từng stream: {handler_type: [(total_bytes, data_list, url_list), ...]}
        classified: dict[str, list] = {'vide': [], 'soun': []}

        for key, url_bs_data_list in streams.items():
            url_list  = [u for u, _, _ in url_bs_data_list]
            data_list = [d for _, _, d in url_bs_data_list]
            handler = None
            for d in data_list:
                handler = self._detect_handler(d)
                if handler:
                    break
            handler = handler or 'vide'
            total_bytes = sum(len(d) for d in data_list)
            classified[handler].append((total_bytes, data_list, url_list))
            logger.debug(
                f"[Playwright] Stream …{key[-40:]}: "
                f"{'AUDIO' if handler == 'soun' else 'VIDEO'} "
                f"({len(data_list)} segs, {total_bytes // 1024}KB)"
            )

        # Adaptive bitrate: chọn stream chất lượng cao nhất (nhiều bytes nhất)
        def _best_stream(streams_list):
            if not streams_list:
                return [], []
            best = max(streams_list, key=lambda x: x[0])
            return best[1], best[2]   # (data_list, url_list)

        video_segs, video_urls = _best_stream(classified['vide'])
        audio_segs, _          = _best_stream(classified['soun'])

        if not video_segs:
            logger.warning("[Playwright] Không thu được video segment nào")
            return None

        video_id = self._video_id_from_url(source_url)
        out_path  = os.path.join(category_dir, f'{video_id}.mp4')

        if not audio_segs:
            # Chỉ có video (không có audio) — ghép trực tiếp, truyền URL để fetch init nếu thiếu
            return self._write_mp4(video_segs, out_path, label='video-only',
                                   seg_urls=video_urls)

        # Cả video + audio — dùng ffmpeg mux
        vtmp = tempfile.NamedTemporaryFile(suffix='_v.mp4', delete=False)
        atmp = tempfile.NamedTemporaryFile(suffix='_a.mp4', delete=False)
        try:
            for d in video_segs:
                vtmp.write(d)
            for d in audio_segs:
                atmp.write(d)
            vtmp.close()
            atmp.close()

            total_mb = (
                sum(len(d) for d in video_segs) + sum(len(d) for d in audio_segs)
            ) / 1024 / 1024
            logger.info(
                f"[Playwright] Mux {len(video_segs)}V + {len(audio_segs)}A segs "
                f"({total_mb:.1f} MB) → {os.path.basename(out_path)}"
            )

            # Thử 1: stream copy (nhanh, không mất chất lượng)
            r1 = subprocess.run(
                ['ffmpeg', '-y', '-fflags', '+genpts',
                 '-i', vtmp.name, '-i', atmp.name,
                 '-c:v', 'copy', '-c:a', 'copy', out_path],
                capture_output=True, timeout=120,
            )
            ok = r1.returncode == 0

            if not ok:
                # Thử 2: re-encode audio để reset DTS (giải quyết DTS không monotonic)
                logger.warning("[Playwright] copy thất bại — re-encode audio")
                r2 = subprocess.run(
                    ['ffmpeg', '-y',
                     '-fflags', '+genpts+discardcorrupt',
                     '-err_detect', 'ignore_err',
                     '-i', vtmp.name, '-i', atmp.name,
                     '-c:v', 'copy', '-c:a', 'aac', '-b:a', '128k', out_path],
                    capture_output=True, timeout=120,
                )
                ok = r2.returncode == 0
                if not ok:
                    # Thử 3: re-encode cả hai (chậm nhất nhưng chắc chắn nhất)
                    logger.warning("[Playwright] re-encode audio thất bại — re-encode cả hai")
                    r3 = subprocess.run(
                        ['ffmpeg', '-y',
                         '-fflags', '+genpts+discardcorrupt',
                         '-err_detect', 'ignore_err',
                         '-i', vtmp.name, '-i', atmp.name,
                         '-c:v', 'libx264', '-crf', '23',
                         '-c:a', 'aac', '-b:a', '128k', out_path],
                        capture_output=True, timeout=300,
                    )
                    ok = r3.returncode == 0
                    if not ok:
                        logger.error(
                            f"[Playwright] ffmpeg lỗi: "
                            f"{r3.stderr.decode(errors='replace')[-300:]}"
                        )
                        return None

            size_mb = os.path.getsize(out_path) / 1024 / 1024
            if size_mb < 0.05:
                logger.warning(f"[Playwright] File quá nhỏ ({size_mb:.2f} MB) — bỏ qua")
                os.remove(out_path)
                return None

            logger.info(f"[Playwright] Saved → {os.path.basename(out_path)} ({size_mb:.1f} MB)")
            return out_path

        finally:
            for f in [vtmp.name, atmp.name]:
                try:
                    os.unlink(f)
                except Exception:
                    pass

    @staticmethod
    def _has_init_segment(segs: list[bytes]) -> bool:
        """
        fMP4 init segment bắt đầu bằng 'ftyp' box (byte 4-7) hoặc chứa 'moov' box.
        Đây là điều kiện đủ để ffmpeg parse được fMP4 stream.
        """
        if not segs:
            return False
        first = segs[0]
        # ftyp ở bytes 4-7 là dấu hiệu chắc chắn của init segment
        if len(first) >= 8 and first[4:8] == b'ftyp':
            return True
        # moov trong 4096 bytes đầu (init segment < 4KB là bình thường)
        if b'moov' in first[:4096]:
            return True
        return False

    @staticmethod
    def _fetch_init_segment(seg_url: str, cookies_file: str) -> bytes | None:
        """
        Cố lấy init segment từ CDN bằng cách request bytestart=0.
        Init segment thường nằm ở bytestart=0, byteend ~700-3000.
        Dùng HTTP Range request để chỉ lấy phần đầu file.
        """
        from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

        parsed = urlparse(seg_url)
        qs = parse_qs(parsed.query)

        # Xây lại URL với bytestart=0, không có byteend (lấy từ đầu)
        qs['bytestart'] = ['0']
        qs.pop('byteend', None)
        new_qs = urlencode({k: v[0] for k, v in qs.items()})
        init_url = urlunparse(parsed._replace(query=new_qs))

        # Load cookies từ Netscape file
        cookies = {}
        if os.path.exists(cookies_file):
            try:
                with open(cookies_file) as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith('#'):
                            continue
                        parts = line.split('\t')
                        if len(parts) >= 7:
                            cookies[parts[5]] = parts[6]
            except Exception:
                pass

        try:
            resp = requests.get(
                init_url,
                cookies=cookies,
                headers={
                    'User-Agent': (
                        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                        '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
                    ),
                    'Range': 'bytes=0-4095',   # chỉ lấy 4KB đầu để có moov
                },
                timeout=15,
            )
            if resp.status_code in (200, 206) and b'ftyp' in resp.content[:12]:
                logger.info(f"[Playwright] Init segment fetched: {len(resp.content)}B")
                return resp.content
        except Exception as e:
            logger.debug(f"[Playwright] Fetch init segment thất bại: {e}")
        return None

    def _write_mp4(self, segs: list, out_path: str, label: str = '',
                   seg_urls: list | None = None) -> str | None:
        """
        Ghép các fMP4 segment thành file MP4 hợp lệ.

        3 lớp fallback:
          1. raw concat → ffmpeg remux (nhanh, đủ khi có init segment)
          2. fetch init segment từ CDN rồi thử lại
          3. ffmpeg concat demuxer với per-segment temp files (chậm nhất, chắc nhất)
        """
        import shutil

        total_mb = sum(len(s) for s in segs) / 1024 / 1024
        logger.info(
            f"[Playwright] Ghép {len(segs)} segment "
            f"({total_mb:.1f} MB{', ' + label if label else ''}) "
            f"→ {os.path.basename(out_path)}"
        )

        has_init = self._has_init_segment(segs)
        if not has_init:
            logger.warning(
                f"[Playwright] Init segment (moov) bị thiếu trong {len(segs)} segment"
            )
            # Cố fetch init segment từ CDN bằng URL của segment đầu tiên
            if seg_urls:
                fetched = self._fetch_init_segment(seg_urls[0], self.cookies_file)
                if fetched and self._has_init_segment([fetched]):
                    segs = [fetched] + list(segs)
                    logger.info("[Playwright] Init segment được fetch thành công — thử lại")
                    has_init = True

        def _run_ffmpeg_raw(seg_list, target):
            """Thử 1: nối thẳng bytes → ffmpeg remux."""
            tmp = target + '.raw.mp4'
            try:
                with open(tmp, 'wb') as f:
                    for s in seg_list:
                        f.write(s)
                r = subprocess.run(
                    ['ffmpeg', '-y',
                     '-fflags', '+genpts+discardcorrupt',
                     '-err_detect', 'ignore_err',
                     '-i', tmp, '-c', 'copy',
                     '-movflags', '+faststart', target],
                    capture_output=True, timeout=120,
                )
                return r.returncode == 0 and os.path.exists(target)
            finally:
                try:
                    os.unlink(tmp)
                except Exception:
                    pass

        def _run_ffmpeg_concat(seg_list, target):
            """Thử 2 / 3: ffmpeg concat demuxer với từng segment là file riêng."""
            tmp_dir = tempfile.mkdtemp(prefix='fb_segs_')
            try:
                seg_paths = []
                for i, data in enumerate(seg_list):
                    p = os.path.join(tmp_dir, f'{i:05d}.mp4')
                    with open(p, 'wb') as f:
                        f.write(data)
                    seg_paths.append(p)

                list_path = os.path.join(tmp_dir, 'concat.txt')
                with open(list_path, 'w') as f:
                    for p in seg_paths:
                        f.write(f"file '{p}'\n")

                r = subprocess.run(
                    ['ffmpeg', '-y',
                     '-f', 'concat', '-safe', '0',
                     '-i', list_path,
                     '-c', 'copy',
                     '-movflags', '+faststart', target],
                    capture_output=True, timeout=120,
                )
                ok = r.returncode == 0 and os.path.exists(target)
                if not ok:
                    logger.warning(
                        f"[Playwright] concat demuxer lỗi: "
                        f"{r.stderr.decode(errors='replace')[-200:]}"
                    )
                return ok
            finally:
                shutil.rmtree(tmp_dir, ignore_errors=True)

        ok = _run_ffmpeg_raw(segs, out_path)

        if not ok:
            logger.warning("[Playwright] raw concat thất bại — thử concat demuxer")
            ok = _run_ffmpeg_concat(segs, out_path)

        if not ok:
            logger.error("[Playwright] Tất cả phương pháp mux đều thất bại")
            return None

        size_mb = os.path.getsize(out_path) / 1024 / 1024
        if size_mb < 0.05:
            logger.warning(f"[Playwright] File quá nhỏ ({size_mb:.2f} MB) — bỏ qua")
            os.remove(out_path)
            return None

        logger.info(f"[Playwright] Saved → {os.path.basename(out_path)} ({size_mb:.1f} MB)")
        return out_path
