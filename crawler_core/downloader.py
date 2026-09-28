"""
GenericDownloader — tải video bằng yt-dlp cho MỌI platform ngoài Facebook.

Facebook cần xử lý riêng (DASH segment capture qua Playwright) nên vẫn dùng
crawler_core.facebook_downloader.VideoDownloader. Cả hai class
có cùng chữ ký `.download(url, category) -> str | None` nên VideoPipeline
dùng được cả hai mà không cần biết đó là platform nào.

Tên file: {extractor}_{id}.{ext} — tránh trùng tên khi 2 platform có cùng
numeric ID (ví dụ TikTok 7123456789 vs Facebook 7123456789).

Chỗ lưu: NAS pvn_share qua GVFS/SMB (xem OUTPUT_BASE). Video LUÔN được tải vào ổ
nội bộ trước rồi mới chuyển sang NAS — bắt buộc, không phải tối ưu: yt-dlp ghi
file phụ .part/.ytdl để ghép mảnh HLS/DASH mà gvfs không chịu được. Chi tiết +
số đo trong GenericDownloader.download().
"""

import logging
import os
import shutil
import subprocess
import tempfile

import yt_dlp

logger = logging.getLogger(__name__)

REPO_ROOT   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COOKIES_DIR = os.path.join(REPO_ROOT, 'cookies')

# ── Nơi lưu video: NAS pvn_share qua GVFS/SMB ────────────────────────────────
# Video được lưu vào <OUTPUT_BASE>/<nhãn>/ (Arson, Robbery, RoadAccident...).
#
# Đường dẫn GVFS phụ thuộc UID nên KHÔNG hardcode '/run/user/1000': suy ra từ
# XDG_RUNTIME_DIR, thiếu thì dựng lại từ os.getuid(). Airflow worker chạy dưới
# cùng user nên trỏ đúng mount của phiên desktop.
#
# Đổi chỗ lưu: export CRAWL_VIDEO_OUTPUT_DIR=/duong/dan/khac
# (dùng cho cả GenericDownloader lẫn VideoDownloader của Facebook)
#
# ⚠ GVFS là FUSE, mount theo PHIÊN ĐĂNG NHẬP: đăng xuất/khởi động lại máy/NAS
# rớt mạng là mount biến mất. Đã kiểm chứng khi share không tồn tại:
# os.makedirs() ném FileNotFoundError chứ KHÔNG âm thầm tạo thư mục rác cục bộ,
# nên task sẽ chết ngay thay vì ghi video vào chỗ sai — ensure_output_base()
# đổi lỗi khó hiểu đó thành thông báo rõ ràng.
#
# Đã đo tốc độ thực tế trên share này (2026-08-04): ghi 8MB ~98 MB/s,
# đọc lại ~27 MB/s, rename .part→.mp4 OK, seek giữa file OK, hash khớp
# — đủ cho yt-dlp (ghi .part rồi rename) và cho L2/L3/L4 đọc lại file.
_NAS_GVFS_SHARE = 'smb-share:server=pvn_nas.local,share=pvn_share'
_NAS_SUBPATH    = os.path.join('tuantq', 'video')


def default_output_base() -> str:
    runtime = os.environ.get('XDG_RUNTIME_DIR') or f'/run/user/{os.getuid()}'
    return os.path.join(runtime, 'gvfs', _NAS_GVFS_SHARE, _NAS_SUBPATH)


OUTPUT_BASE = os.environ.get('CRAWL_VIDEO_OUTPUT_DIR') or default_output_base()

# Thư mục cũ — chỉ để thông báo cho người vận hành, KHÔNG tự đọc/ghi vào đây nữa.
LEGACY_OUTPUT_BASE = os.path.join(REPO_ROOT, 'data', 'videos')

# Nơi TẢI TẠM (phải là ổ NỘI BỘ — xem giải thích trong GenericDownloader.download).
# Mỗi lượt tải dùng 1 thư mục con riêng và tự xóa sau khi chuyển file đi, nên chỗ
# chiếm chỉ bằng 1 video (trần CRAWL max_filesize_mb, mặc định 500 MB).
STAGING_DIR = os.environ.get('CRAWL_DOWNLOAD_STAGING_DIR') or os.path.join(
    REPO_ROOT, 'data', '.staging'
)


# ── Nơi chứa video BỊ LOẠI (CRAWL_REJECT_ACTION=move — xem crawler_core/pipeline.py)
# Cấu trúc: <REJECTED_BASE>/<lý do>/<nhãn>/<file>.mp4 + <file>.json (lý do chi tiết).
#
# ⚠ PHẢI nằm NGOÀI OUTPUT_BASE. scripts/crawl_report_24h.py (collect_disk) đếm MỌI
# thư mục con của OUTPUT_BASE là 1 nhãn, và scripts/backfill_fingerprints.py
# --scan-dir os.walk toàn bộ cây — để thư mục loại bên trong thì video bị loại bị
# đếm vào dataset và bị lấy fingerprint như video thật. Mặc định: thư mục anh em
# của OUTPUT_BASE (NAS: tuantq/video_rejected cạnh tuantq/video).
REJECTED_BASE = os.environ.get('CRAWL_REJECTED_DIR') or os.path.join(
    os.path.dirname(OUTPUT_BASE.rstrip(os.sep)), 'video_rejected'
)


def ensure_staging_dir() -> str:
    os.makedirs(STAGING_DIR, exist_ok=True)
    return STAGING_DIR


def move_file(src: str, dest_dir: str) -> str | None:
    """
    Chuyển file sang dest_dir (tạo nếu chưa có), trả đường dẫn mới, None nếu lỗi.

    copy_function=shutil.copyfile là BẮT BUỘC khi đích là NAS. Mặc định
    shutil.move dùng copy2 → copystat() → chmod/utime, mà gvfs/SMB không hỗ trợ:
    OSError [Errno 95] Operation not supported (đã tái hiện 2026-08-04, file tải
    xong rồi vẫn hỏng ở bước chuyển). copyfile chỉ chép nội dung, không đụng
    metadata. Cùng ổ thì move vẫn dùng os.rename (tức thì), không copy.
    """
    dest = os.path.join(dest_dir, os.path.basename(src))
    try:
        os.makedirs(dest_dir, exist_ok=True)
        shutil.move(src, dest, copy_function=shutil.copyfile)
        return dest
    except Exception as e:
        logger.error(
            f"[Download] Không chuyển được file sang {dest_dir}: "
            f"{type(e).__name__}: {e}"
        )
        return None


def release_staged(path: str | None):
    """
    Xóa thư mục tạm riêng của 1 lượt fetch() (kèm file bên trong nếu chưa được
    chuyển đi). Chỉ xóa khi thư mục đó nằm trong STAGING_DIR — không bao giờ
    rmtree nhầm thư mục dataset nếu lỡ truyền vào đường dẫn đã chuyển sang NAS.
    """
    if not path:
        return
    parent = os.path.dirname(os.path.abspath(path))
    staging_root = os.path.abspath(STAGING_DIR)
    if os.path.dirname(parent) == staging_root:
        shutil.rmtree(parent, ignore_errors=True)


def ensure_output_base(path: str = None) -> str:
    """
    Tạo thư mục lưu video, báo lỗi RÕ RÀNG nếu NAS chưa mount.

    Không có hàm này thì lỗi hiện ra là 'FileNotFoundError: [Errno 2] No such
    file or directory: /run/user/1000/gvfs/smb-share:server=...' — nhìn như bug
    code chứ không gợi ý gì tới việc phải mount lại NAS.
    """
    path = path or OUTPUT_BASE
    try:
        os.makedirs(path, exist_ok=True)
        return path
    except OSError as e:
        gvfs_root = os.path.dirname(os.path.dirname(path))
        raise RuntimeError(
            f"Không tạo/truy cập được thư mục lưu video:\n"
            f"    {path}\n"
            f"    ({type(e).__name__}: {e})\n"
            f"NAS pvn_share nhiều khả năng CHƯA MOUNT. GVFS mount theo phiên đăng "
            f"nhập desktop nên mất sau khi đăng xuất/khởi động lại.\n"
            f"Cách xử lý:\n"
            f"  1. Mở Files (Nautilus) → Other Locations → nhập:\n"
            f"     smb://pvn_nas.local/pvn_share\n"
            f"     rồi đăng nhập; hoặc chạy: gio mount smb://pvn_nas.local/pvn_share\n"
            f"  2. Kiểm tra: ls '{gvfs_root}'\n"
            f"  3. Hoặc lưu tạm vào ổ nội bộ: "
            f"export CRAWL_VIDEO_OUTPUT_DIR={LEGACY_OUTPUT_BASE}"
        ) from e

# Cookie file cho từng platform. Ưu tiên env {PLATFORM}_COOKIES_FILE,
# sau đó tới đường dẫn mặc định bên dưới.
_COOKIE_PATHS = {
    'facebook':    os.path.join(COOKIES_DIR, 'facebook_legacy_cookies.txt'),
    'youtube':     os.path.join(COOKIES_DIR, 'youtube_cookies.txt'),
    'tiktok':      os.path.join(COOKIES_DIR, 'tiktok_cookies.txt'),
    'instagram':   os.path.join(COOKIES_DIR, 'instagram_cookies.txt'),
    'x':           os.path.join(COOKIES_DIR, 'x_cookies.txt'),
    'reddit':      os.path.join(COOKIES_DIR, 'reddit_cookies.txt'),
    'vimeo':       os.path.join(COOKIES_DIR, 'vimeo_cookies.txt'),
    'dailymotion': os.path.join(COOKIES_DIR, 'dailymotion_cookies.txt'),
}


# ── Dailymotion: TUYỆT ĐỐI KHÔNG truyền cookiefile khi tải ──────────────────
# Ý tưởng cũ: yt-dlp xin access_token bằng client_id hardcode dùng chung toàn cầu
# ('f1a362d288c1b98099c7') nên hay bị rate-limit 401; `_get_token()` đọc cookie
# 'client_token' TRƯỚC khi gọi API, vậy cache token vào 1 file để tái dùng.
#
# THỰC TẾ NÓ PHẢN TÁC DỤNG — đo 2026-08-03 trên cùng 3 video, cùng lúc:
#     cookiefile = dailymotion_token_cache.txt   → 401 Unauthorized  3/3
#     cookiefile = dailymotion_cookies.txt       → 401 Unauthorized  3/3
#     KHÔNG truyền cookiefile                    → OK               3/3
#     cookiefile trống mới tinh                  → OK               3/3
#
# Vì `_get_token()` (yt_dlp/extractor/dailymotion.py:54) đọc
# `access_token` **rồi mới** `client_token` từ cookie và dùng thẳng làm
# `Authorization: Bearer` — token trong file đã chết thì nó gửi token chết đó
# MÃI MÃI, không bao giờ tự xin token mới. Cache biến thành "ghim token chết".
# Đây chính là thứ làm 207/225 lượt tải của lần chạy 3h ngày 2026-08-03 trả 401
# và downloaded=0.
#
# ⚠ Cũng ĐỪNG trỏ vào cookies/dailymotion_cookies.txt: (1) access_token của
# phiên web vẫn 401 như đo ở trên, (2) yt-dlp ghi cả jar ngược vào cookiefile —
# đúng cơ chế đã phá hỏng cookies/facebook_legacy_cookies.txt trước đây, sẽ mất
# luôn refresh_token (thứ chỉ lấy lại được bằng đăng nhập tay).
#
# Nếu về sau endpoint token thật sự bị rate-limit, cách đúng là cache có khả
# năng TỰ HỦY khi gặp 401, KHÔNG phải ghim vĩnh viễn như trước.
_DAILYMOTION_TOKEN_CACHE = os.path.join(COOKIES_DIR, 'dailymotion_token_cache.txt')


def cookies_file_for(platform: str | None) -> str | None:
    """
    Cookie định dạng Netscape cho yt-dlp. None nếu chưa có file.
    Tạo bằng: python scripts/save_platform_cookies.py <platform>

    Dailymotion là ngoại lệ: luôn trả None — truyền cookiefile cho yt-dlp làm
    nó gửi token đã chết và 401 mọi lần. Xem khối giải thích + số đo ở
    _DAILYMOTION_TOKEN_CACHE.
    """
    if platform == 'dailymotion':
        return None
    if not platform:
        return None
    env_key = f'{platform.upper()}_COOKIES_FILE'
    path = os.environ.get(env_key) or _COOKIE_PATHS.get(platform)
    return path if path and os.path.exists(path) else None


def cookies_json_for(platform: str | None) -> str | None:
    """
    Cookie định dạng JSON cho Playwright. None nếu chưa có file.
    Cùng script tạo ra cả 2 định dạng nên thường có hoặc thiếu cùng lúc.
    """
    if not platform:
        return None
    env_key = f'{platform.upper()}_COOKIES_JSON'
    path = os.environ.get(env_key) or os.path.join(
        COOKIES_DIR, f'{platform}_cookies.json'
    )
    return path if os.path.exists(path) else None


class GenericDownloader:
    """
    Args:
        platform      : dùng để chọn cookie file phù hợp
        max_duration  : bỏ qua video dài hơn N giây (0 = không giới hạn).
                        Chặn tải nhầm phim/stream dài hàng giờ.
        max_filesize_mb: giới hạn dung lượng, tránh nghẽn ổ đĩa
    """

    def __init__(
        self,
        platform: str | None = None,
        output_base: str = OUTPUT_BASE,
        max_duration: int = 1800,
        max_filesize_mb: int = 500,
    ):
        self.platform        = platform
        self.output_base     = output_base
        self.max_duration    = max_duration
        self.max_filesize_mb = max_filesize_mb
        # Kiểm tra NGAY lúc khởi tạo (đầu task) chứ không đợi tới video đầu tiên:
        # NAS chưa mount thì task chết sớm với thông báo rõ, không phí công
        # discovery + VLM rồi mới hỏng ở bước ghi file.
        ensure_output_base(output_base)
        logger.info(f"[Download] Lưu video vào: {output_base}")

    # ── API dùng bởi VideoPipeline ─────────────────────────────────────

    def fetch(self, url: str) -> str | None:
        """
        Tải video vào thư mục TẠM trên ổ nội bộ, KHÔNG chuyển đi đâu cả.

        Trả đường dẫn file (nằm trong 1 thư mục tạm riêng dưới STAGING_DIR), hoặc
        None nếu tải hỏng/file không playable. Caller chịu trách nhiệm chuyển file
        đi (move_file) rồi gọi release_staged(path) để dọn thư mục tạm.

        VideoPipeline dùng hàm này để chạy mọi bộ lọc trên file ở ổ nội bộ (đọc
        nhanh hơn NAS ~27 MB/s) rồi mới quyết định file vào dataset hay thư mục loại.
        """
        staging = tempfile.mkdtemp(prefix='dl_', dir=ensure_staging_dir())
        # Lý do phải tải vào ổ nội bộ: xem download() bên dưới.
        path = self._download_ytdlp(url, staging)
        if not path:
            shutil.rmtree(staging, ignore_errors=True)
            return None
        if not self._has_video_stream(path):
            logger.warning(f"[Download] File không playable — bỏ: {url}")
            shutil.rmtree(staging, ignore_errors=True)
            return None
        return path

    def download(self, url: str, category: str = 'CCTV') -> str | None:
        category_dir = os.path.join(self.output_base, category)
        os.makedirs(category_dir, exist_ok=True)

        # TẢI VÀO Ổ NỘI BỘ TRƯỚC, xong xuôi mới chuyển sang đích.
        # BẮT BUỘC khi đích là ổ mạng (NAS qua GVFS/SMB): yt-dlp tải video dạng
        # nhiều mảnh (HLS/DASH) bằng cách ghi file phụ .part và .ytdl rồi ghép
        # lại — thao tác đó KHÔNG chạy trên gvfs. Đã tái hiện 2026-08-04:
        #     [hlsnative] Total fragments: 5
        #     ERROR: Unable to download video: [Errno 2] No such file or
        #            directory: '.../dailymotion_x89ji42.mp4.ytdl'
        # Lần khác thì ra file 4 MB nhưng hỏng, bị _has_video_stream() loại rồi
        # xóa — nhìn như "video lỗi" chứ không lộ ra là do ổ mạng.
        # Tải nội bộ còn khiến ffprobe + hash (L2/L3/L4) đọc trên ổ nhanh, chỉ
        # tốn đúng 1 lần copy tuần tự sang NAS (đo được ~98 MB/s).
        path = self.fetch(url)
        if not path:
            return None
        try:
            return move_file(path, category_dir)
        finally:
            release_staged(path)

    # ── yt-dlp ─────────────────────────────────────────────────────────

    def _download_ytdlp(self, url: str, category_dir: str) -> str | None:
        opts = {
            'outtmpl': os.path.join(category_dir, '%(extractor)s_%(id)s.%(ext)s'),
            # Ưu tiên file có sẵn cả video+audio để không phải merge
            'format': (
                'best[ext=mp4][vcodec!=none][acodec!=none]'
                '/best[ext=mp4]'
                '/bestvideo[ext=mp4]+bestaudio[ext=m4a]'
                '/best'
            ),
            'merge_output_format': 'mp4',
            'fixup': 'detect_or_warn',
            'quiet': True,
            'no_warnings': True,
            'noprogress': True,
            'retries': 3,
            'fragment_retries': 5,
            'socket_timeout': 30,
            # Không tải playlist khi URL vô tình là link playlist
            'noplaylist': True,
            # Bỏ qua live stream — pipeline này dùng cho video đã ghi
            'match_filter': self._build_match_filter(),
        }
        if self.max_filesize_mb:
            opts['max_filesize'] = self.max_filesize_mb * 1024 * 1024

        cookies = cookies_file_for(self.platform)
        if cookies:
            opts['cookiefile'] = cookies

        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=True)
                if not info:
                    return None
                if info.get('entries'):          # phòng trường hợp vẫn ra playlist
                    entries = [e for e in info['entries'] if e]
                    if not entries:
                        return None
                    info = entries[0]
                filename = ydl.prepare_filename(info)
                for ext in ('.mp4', '.mkv', '.webm', ''):
                    candidate = os.path.splitext(filename)[0] + ext
                    if os.path.exists(candidate):
                        size_mb = os.path.getsize(candidate) / 1024 / 1024
                        logger.info(
                            f"[yt-dlp] {os.path.basename(candidate)} ({size_mb:.1f} MB)"
                        )
                        return candidate
        except Exception as e:
            logger.debug(f"[yt-dlp] {url}: {e}")
        return None

    def _build_match_filter(self):
        """Loại live stream và video quá dài NGAY TRƯỚC khi tải."""
        max_dur = self.max_duration

        def _filt(info_dict, *, incomplete=False):
            if info_dict.get('is_live'):
                return 'đang live — bỏ qua'
            if info_dict.get('live_status') in ('is_live', 'is_upcoming'):
                return 'live/upcoming — bỏ qua'
            dur = info_dict.get('duration')
            if max_dur and dur and dur > max_dur:
                return f'dài {dur:.0f}s > giới hạn {max_dur}s — bỏ qua'
            return None

        return _filt

    # ── Helpers ────────────────────────────────────────────────────────

    @staticmethod
    def _has_video_stream(file_path: str) -> bool:
        try:
            r = subprocess.run(
                ['ffprobe', '-v', 'error', '-select_streams', 'v:0',
                 '-show_entries', 'stream=codec_type', '-of', 'csv=p=0', file_path],
                capture_output=True, timeout=30,
            )
            return r.returncode == 0 and b'video' in r.stdout
        except Exception:
            return False

    @staticmethod
    def _remove(path: str):
        try:
            os.remove(path)
        except Exception:
            pass
