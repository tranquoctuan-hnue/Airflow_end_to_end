"""
Video deduplication — 4 tầng lọc trùng lặp, dùng chung cho MỌI platform.

  L1 — Canonical ID : (source_platform, canonical_object_type, canonical_object_id)
                      Chỉ bắt trùng TRONG CÙNG platform. Chạy TRƯỚC download
                      và trước VLM → rẻ nhất (1 SQL lookup có index).

  L2 — File hash    : SHA-256 toàn file. Bắt bản sao byte-by-byte.
                      Chạy sau download.  Chi phí ~10ms/100MB.

  L3 — Thumbnail    : phash 1 frame tại 50% duration. Bắt trùng rõ ràng
                      dù khác encoding/bitrate.  Chi phí ~0.3s.

  L4 — Fingerprint  : phash 5 frame tại 10/30/50/70/90% duration.
                      Chính xác nhất — chịu được watermark, crop nhẹ,
                      re-encode.  Chi phí ~1.5s.

QUAN TRỌNG — dedup CHÉO platform:
  L1 chỉ so trong cùng platform (video:123 của FB ≠ video:123 của TikTok).
  L2/L3/L4 KHÔNG filter theo platform → chính chúng bắt được cùng một clip
  CCTV được đăng lại trên Facebook, TikTok và YouTube.

Nếu imagehash chưa cài, L3/L4 tự bỏ qua — pipeline vẫn chạy với L1/L2.
"""

import hashlib
import logging
import os
import re
import shutil
import subprocess
import tempfile
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# ── Ngưỡng so sánh ───────────────────────────────────────────────────────────
THUMBNAIL_THRESHOLD   = 8     # L3: phash distance < 8/64  → cùng nội dung
FINGERPRINT_THRESHOLD = 10    # L4: avg distance   < 10/64 → cùng nội dung

# Pre-filter duration cho L3/L4: chỉ đem so phash những clip có độ dài xấp xỉ.
#
# ⚠ ĐÂY LÀ RÀO CHẶN, KHÔNG PHẢI TIÊU CHÍ NHẬN DẠNG. Nó nằm trong mệnh đề
# `WHERE duration BETWEEN ? AND ?` của find_by_thumbnail/find_by_fingerprint,
# nên clip nằm ngoài cửa sổ bị loại TRƯỚC khi phash kịp so sánh — không tầng nào
# chạy. Đặt quá hẹp là vô hiệu hóa cả L3 lẫn L4 mà không có log nào báo.
#
# Sự cố 2026-08-10: cùng một clip Abuse được repost bởi 2 tài khoản X khác nhau
# (PoojaTalwar21 25.73s/720p/4.7MB và nextminutenews7 28.16s/1080p/17.3MB) đều
# được tải về. Truy vết từng tầng:
#     L1  không bắt được — 2 tweet ID khác nhau, đúng như thiết kế
#     L2  không bắt được — khác encode nên khác sha256, đúng như thiết kế
#     L3  distance 10/64 ≥ ngưỡng 8  → trượt (frame giữa lệch do cắt khác nhau)
#     L4  distance 5.2/64 < ngưỡng 10 → ĐÁNG LẼ BẮT ĐƯỢC
# nhưng 2 clip lệch 8.6% độ dài, ngoài cửa sổ ±5% cũ, nên L4 không hề được chạy.
#
# Đo trên mẫu 300 fingerprint ngẫu nhiên (bảng 6298 dòng):
#     ±5%  →  0 cặp trùng phát hiện được   (L4 gần như vô dụng)
#     ±20% →  2 cặp, cả hai đều trùng thật (lệch 8.6% và 16.4%), không dương
#             tính giả; chi phí quét 658 dòng/70ms thay vì 157 dòng/92ms
# Chính phash mới là thứ quyết định trùng hay không; rào này chỉ để khỏi quét
# cả bảng. Nới rộng làm tăng số dòng phải quét TUYẾN TÍNH, nên nếu bảng phình
# lên hàng trăm nghìn dòng thì cân nhắc hạ lại hoặc thêm index/phân cụm.
DURATION_TOLERANCE = float(
    os.environ.get('CRAWL_DEDUP_DURATION_TOLERANCE') or 0.20
)


# ═══════════════════════════════════════════════════════════════════════════════
# Layer 1 — Canonical ID  (platform + object_type + object_id)
# ═══════════════════════════════════════════════════════════════════════════════
#
# Mỗi platform: (tập hostname, danh sách rule)
# Rule = (regex, canonical_object_type, is_short_link)
#   - Thứ tự rule QUAN TRỌNG: pattern cụ thể hơn phải đứng trước.
#   - is_short_link=True → URL rút gọn, ID trong đó KHÁC canonical ID thật.
#     Pipeline sẽ resolve redirect trước khi tra L1 (xem resolve_short_url).

_YT_ID = r'[A-Za-z0-9_\-]{11}'      # YouTube video ID: đúng 11 ký tự, phân biệt hoa/thường

_PLATFORM_RULES: dict[str, tuple[set[str], list[tuple[str, str, bool]]]] = {

    'facebook': (
        {'facebook.com', 'fb.com', 'fb.watch', 'm.facebook.com', 'web.facebook.com'},
        [
            # Reel phải đứng trước video: một số URL reel chứa cả '/videos/'
            (r'/reel/(\d+)',                       'reel',  False),
            (r'/share/r/([A-Za-z0-9_\-]+)',        'reel',  True),
            (r'[?&]v=(\d+)',                       'video', False),
            (r'/videos/(\d+)',                     'video', False),
            (r'/share/v/([A-Za-z0-9_\-]+)',        'video', True),
            (r'fb\.watch/([A-Za-z0-9_\-]+)',       'video', True),
            (r'story_fbid=(\d+)',                  'post',  False),
            (r'[?&]fbid=(\d+)',                    'photo', False),
            (r'/photos?/(\d+)',                    'photo', False),
        ],
    ),

    'youtube': (
        {'youtube.com', 'youtu.be', 'm.youtube.com',
         'music.youtube.com', 'youtube-nocookie.com'},
        [
            (rf'/shorts/({_YT_ID})',               'short', False),
            (rf'[?&]v=({_YT_ID})',                 'video', False),
            (rf'youtu\.be/({_YT_ID})',             'video', False),
            (rf'/embed/({_YT_ID})',                'video', False),
            (rf'/live/({_YT_ID})',                 'video', False),
        ],
    ),

    'tiktok': (
        {'tiktok.com', 'vm.tiktok.com', 'vt.tiktok.com', 'm.tiktok.com'},
        [
            (r'/@[^/]+/video/(\d+)',               'video', False),
            (r'/@[^/]+/photo/(\d+)',               'photo', False),
            (r'/v/(\d+)',                          'video', False),
            (r'vm\.tiktok\.com/([A-Za-z0-9]+)',    'video', True),
            (r'vt\.tiktok\.com/([A-Za-z0-9]+)',    'video', True),
            (r'/t/([A-Za-z0-9]+)',                 'video', True),
        ],
    ),

    'instagram': (
        {'instagram.com', 'instagr.am', 'www.instagram.com'},
        [
            (r'/reels?/([A-Za-z0-9_\-]+)',         'reel',  False),
            (r'/tv/([A-Za-z0-9_\-]+)',             'video', False),
            (r'/stories/[^/]+/(\d+)',              'story', False),
            (r'/p/([A-Za-z0-9_\-]+)',              'post',  False),
        ],
    ),

    'x': (
        {'x.com', 'twitter.com', 'mobile.twitter.com', 'mobile.x.com'},
        [
            (r'/i/status/(\d+)',                   'post',  False),
            (r'/status(?:es)?/(\d+)',              'post',  False),
        ],
    ),

    'reddit': (
        {'reddit.com', 'old.reddit.com', 'new.reddit.com',
         'redd.it', 'v.redd.it'},
        [
            (r'v\.redd\.it/([A-Za-z0-9]+)',        'video', False),
            (r'/r/[^/]+/comments/([a-z0-9]+)',     'post',  False),
            (r'/comments/([a-z0-9]+)',             'post',  False),
            (r'redd\.it/([a-z0-9]+)',              'post',  True),
        ],
    ),

    'vimeo': (
        {'vimeo.com', 'player.vimeo.com'},
        [
            (r'/video/(\d+)',                      'video', False),
            (r'vimeo\.com/(\d+)',                  'video', False),
        ],
    ),

    'dailymotion': (
        {'dailymotion.com', 'dai.ly', 'www.dailymotion.com'},
        [
            (r'/video/([A-Za-z0-9]+)',             'video', False),
            (r'dai\.ly/([A-Za-z0-9]+)',            'video', True),
        ],
    ),
}

# Hostname → tên platform (build 1 lần khi import)
_HOST_TO_PLATFORM: dict[str, str] = {
    host: platform
    for platform, (hosts, _) in _PLATFORM_RULES.items()
    for host in hosts
}


def detect_platform(url: str) -> str | None:
    """
    Nhận diện platform từ hostname (khớp chính xác hoặc subdomain).
    Không dùng substring match trên cả URL — tránh false positive kiểu
    'https://evil.com/?ref=facebook.com'.
    """
    try:
        netloc = urlparse(url).netloc.lower().split('@')[-1].split(':')[0]
    except Exception:
        return None
    if not netloc:
        return None

    if netloc in _HOST_TO_PLATFORM:
        return _HOST_TO_PLATFORM[netloc]

    # Bỏ tiền tố 'www.' rồi thử lại
    if netloc.startswith('www.'):
        bare = netloc[4:]
        if bare in _HOST_TO_PLATFORM:
            return _HOST_TO_PLATFORM[bare]

    # Khớp subdomain: 'de.vimeo.com' → 'vimeo.com'
    for host, platform in _HOST_TO_PLATFORM.items():
        if netloc.endswith('.' + host):
            return platform
    return None


def extract_canonical_id(url: str) -> tuple[str, str, str] | None:
    """
    Chuẩn hóa URL → (source_platform, canonical_object_type, canonical_object_id).

      facebook.com/watch?v=123                → ('facebook',    'video', '123')
      facebook.com/groups/g/videos/123/?ido=g → ('facebook',    'video', '123')
      facebook.com/reel/123                   → ('facebook',    'reel',  '123')
      youtube.com/watch?v=dQw4w9WgXcQ         → ('youtube',     'video', 'dQw4w9WgXcQ')
      youtube.com/shorts/dQw4w9WgXcQ          → ('youtube',     'short', 'dQw4w9WgXcQ')
      tiktok.com/@user/video/7123456789       → ('tiktok',      'video', '7123456789')
      instagram.com/reel/AbC-1_x              → ('instagram',   'reel',  'AbC-1_x')
      x.com/user/status/1789                  → ('x',           'post',  '1789')
      reddit.com/r/sub/comments/1a2b3c/title  → ('reddit',      'post',  '1a2b3c')
      vimeo.com/123456                        → ('vimeo',       'video', '123456')
      dailymotion.com/video/x8abcd            → ('dailymotion', 'video', 'x8abcd')

    Trả None nếu không nhận diện được platform hoặc object.
    """
    platform = detect_platform(url)
    if not platform:
        return None

    for pattern, obj_type, _is_short in _PLATFORM_RULES[platform][1]:
        m = re.search(pattern, url)
        if m:
            return (platform, obj_type, m.group(1))
    return None


def is_short_link(url: str) -> bool:
    """
    True nếu URL là dạng rút gọn (vm.tiktok.com/xxx, fb.watch/xxx, dai.ly/xxx...).
    ID trong short link KHÁC canonical ID thật → nên resolve redirect trước L1,
    nếu không cùng một video qua 2 dạng URL sẽ lọt L1 (vẫn bị L2/L3/L4 bắt).
    """
    platform = detect_platform(url)
    if not platform:
        return False
    for pattern, _obj_type, is_short in _PLATFORM_RULES[platform][1]:
        if re.search(pattern, url):
            return is_short
    return False


def resolve_short_url(url: str, timeout: int = 10) -> str:
    """
    Theo redirect của short link để lấy URL đầy đủ (canonical).
    Trả về URL gốc nếu resolve thất bại — không bao giờ raise.
    """
    try:
        import requests
        resp = requests.head(
            url, allow_redirects=True, timeout=timeout,
            headers={'User-Agent': (
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
            )},
        )
        if resp.url and resp.url != url:
            logger.debug(f"[Dedup] short link resolved: {url} → {resp.url}")
            return resp.url
    except Exception as e:
        logger.debug(f"[Dedup] resolve short link thất bại ({url}): {e}")
    return url


# ═══════════════════════════════════════════════════════════════════════════════
# Layer 2 — File hash
# ═══════════════════════════════════════════════════════════════════════════════

def compute_file_hash(file_path: str) -> str | None:
    """SHA-256 toàn file — phát hiện bản sao byte-by-byte. None nếu đọc lỗi."""
    try:
        sha = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(1 << 20), b''):
                sha.update(chunk)
        return sha.hexdigest()
    except Exception as e:
        logger.warning(f"[Dedup] Không tính được file hash: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers dùng chung cho Layer 3 & 4
# ═══════════════════════════════════════════════════════════════════════════════

def get_video_duration(file_path: str) -> float:
    """Duration (giây) qua ffprobe. Trả 0.0 nếu lỗi."""
    try:
        r = subprocess.run(
            ['ffprobe', '-v', 'error', '-show_entries', 'format=duration',
             '-of', 'csv=p=0', file_path],
            capture_output=True, timeout=15,
        )
        if r.returncode == 0 and r.stdout.strip():
            return float(r.stdout.strip())
    except Exception:
        pass
    return 0.0


def _extract_frame(file_path: str, timestamp: float, out_path: str) -> bool:
    try:
        r = subprocess.run(
            ['ffmpeg', '-y', '-ss', f'{timestamp:.3f}', '-i', file_path,
             '-vframes', '1', '-q:v', '2', out_path],
            capture_output=True, timeout=20,
        )
        return r.returncode == 0 and os.path.exists(out_path)
    except Exception:
        return False


_IMAGEHASH_WARNED = False


def _phash_str(img_path: str) -> str | None:
    """phash → hex string. None nếu imagehash chưa cài hoặc ảnh lỗi."""
    global _IMAGEHASH_WARNED
    try:
        import imagehash
        from PIL import Image
        return str(imagehash.phash(Image.open(img_path)))
    except ImportError:
        if not _IMAGEHASH_WARNED:
            logger.warning(
                "[Dedup] imagehash chưa cài — L3/L4 bị tắt. "
                "Cài bằng: pip install imagehash"
            )
            _IMAGEHASH_WARNED = True
        return None
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# Layer 3 — Thumbnail hash (1 frame tại 50% duration)
# ═══════════════════════════════════════════════════════════════════════════════

def compute_thumbnail_hash(file_path: str, duration: float = 0.0) -> str | None:
    """L3: phash frame giữa video. Nhanh (~0.3s), bắt trùng rõ ràng."""
    if duration <= 0:
        duration = get_video_duration(file_path)
    if duration < 0.5:
        return None

    tmp = tempfile.NamedTemporaryFile(suffix='.jpg', delete=False)
    tmp.close()
    try:
        if _extract_frame(file_path, duration * 0.5, tmp.name):
            return _phash_str(tmp.name)
    finally:
        try:
            os.unlink(tmp.name)
        except Exception:
            pass
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# Layer 4 — 5-frame fingerprint (10/30/50/70/90% duration)
# ═══════════════════════════════════════════════════════════════════════════════

FRAME_POSITIONS = [0.1, 0.3, 0.5, 0.7, 0.9]


def compute_fingerprint(file_path: str, duration: float = 0.0) -> str | None:
    """
    L4: phash 5 frame → 'h1:h2:h3:h4:h5'.
    Cần ≥3 frame thành công; ít hơn trả None để tránh false positive.
    """
    if duration <= 0:
        duration = get_video_duration(file_path)
    if duration < 0.5:
        return None

    hashes = []
    tmp_dir = tempfile.mkdtemp(prefix='dedup_')
    try:
        for pos in FRAME_POSITIONS:
            out = os.path.join(tmp_dir, f'f{int(pos * 100):03d}.jpg')
            if _extract_frame(file_path, duration * pos, out):
                h = _phash_str(out)
                if h is not None:
                    hashes.append(h)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    return ':'.join(hashes) if len(hashes) >= 3 else None


# ═══════════════════════════════════════════════════════════════════════════════
# Distance helpers — DBManager dùng khi so hash lấy từ DB
# ═══════════════════════════════════════════════════════════════════════════════

def phash_distance(h1: str, h2: str) -> int:
    """Hamming distance giữa 2 phash hex string (0–64). 64 nếu lỗi."""
    try:
        import imagehash
        return imagehash.hex_to_hash(h1) - imagehash.hex_to_hash(h2)
    except Exception:
        return 64


def fingerprint_avg_distance(fp1: str, fp2: str) -> float:
    """Average Hamming distance của các cặp frame giữa 2 fingerprint."""
    try:
        pairs = list(zip(fp1.split(':'), fp2.split(':')))
        if not pairs:
            return 64.0
        return sum(phash_distance(a, b) for a, b in pairs) / len(pairs)
    except Exception:
        return 64.0


# ═══════════════════════════════════════════════════════════════════════════════
# Portrait filter — dùng chung (CCTV thật gần như luôn là landscape)
# ═══════════════════════════════════════════════════════════════════════════════

def video_dimensions(file_path: str) -> tuple[int, int] | None:
    """(width, height) của video stream đầu tiên, None nếu không đọc được."""
    import json
    try:
        r = subprocess.run(
            ['ffprobe', '-v', 'quiet', '-show_streams', '-of', 'json', file_path],
            capture_output=True, timeout=10,
        )
        for s in json.loads(r.stdout).get('streams', []):
            if s.get('codec_type') == 'video':
                return s.get('width', 0), s.get('height', 0)
    except Exception:
        pass
    return None


def is_portrait_video(file_path: str, tolerance: float = 0.05) -> bool:
    """True nếu video thực tế có tỉ lệ 9:16 (quay dọc — Reels/Shorts/TikTok)."""
    dims = video_dimensions(file_path)
    if not dims:
        return False
    w, h = dims
    return h > 0 and abs((w / h) - (9 / 16)) < tolerance


def extract_frame(file_path: str, timestamp: float, out_path: str) -> bool:
    """Trích 1 frame tại `timestamp` (giây) ra ảnh JPEG. True nếu thành công."""
    return _extract_frame(file_path, timestamp, out_path)
