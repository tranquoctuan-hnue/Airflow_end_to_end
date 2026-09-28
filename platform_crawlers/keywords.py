"""
Sinh từ khóa search THEO TỪNG NHÃN (11 nhãn trong platform_crawlers/labels.py).

Video tìm được bằng keyword của nhãn nào sẽ được lưu vào thư mục nhãn đó:
    data/videos/Arson/        data/videos/Robbery/       data/videos/Shooting/
    data/videos/RoadAccident/ data/videos/Shoplifting/   ...

BA DẠNG TỪ KHÓA — không dùng lẫn được:

    SEARCH_PHRASE   YouTube, Dailymotion, X, Reddit(search)
                    Cụm từ có dấu cách: "cctv ghi lại vụ phóng hỏa"

    HASHTAG         TikTok, Instagram
                    Một token, KHÔNG dấu cách, KHÔNG '#': "cctvphonghoa"
                    Hai platform này search qua trang /tag/<hashtag> nên cụm
                    từ có dấu cách sẽ ra 0 kết quả.

    SUBREDDIT       Reddit
                    Tên community (r/CaughtOnCamera) — KHÔNG sinh tự động
                    được vì phải là sub tồn tại thật → CRAWL_REDDIT_SUBS.

NGUỒN, theo thứ tự ưu tiên:
    1. Env chỉ định tay  CRAWL_YOUTUBE_KEYWORDS="a|b"  → dùng luôn, KHÔNG gán nhãn
                         (video vào thư mục mặc định, dùng cho lần chạy ad-hoc)
    2. Gemini            CRAWL_LLM_KEYWORDS=true (default) → sinh cho từng nhãn
    3. List tĩnh         labels.py → LABELS[x]['en'], khi Gemini lỗi/hết quota

Dùng lại hạ tầng Gemini có sẵn (crawler_core.gemini_client): 10 API key
xoay vòng + fallback nhiều model khi 1 cặp (key, model) hết quota.
CHỈ MỘT lần gọi Gemini cho cả 11 nhãn, không phải 11 lần.

QUAN TRỌNG: gọi các hàm này TRONG task (trong discover()), không ở module
level — Airflow parse file DAG mỗi ~30 giây, sinh keyword lúc import sẽ gọi
Gemini liên tục và đốt hết quota.
"""

import logging
import os
import random
import re
import unicodedata

from platform_crawlers import labels as L

logger = logging.getLogger(__name__)

USE_LLM = os.environ.get('CRAWL_LLM_KEYWORDS', 'true').lower() == 'true'

# Số keyword cho MỖI nhãn. 11 nhãn × 3 = 33 lượt search/platform.
# Tăng lên sẽ tìm được nhiều hơn nhưng lâu hơn tỉ lệ thuận.
PER_LABEL = int(os.environ.get('CRAWL_KEYWORDS_PER_LABEL', '3') or 3)

# Cache trong process: 1 task Airflow = 1 process → mỗi lần chạy task chỉ gọi
# Gemini 1 lần cho mỗi dạng, dù discover() lặp qua nhiều nhãn × nhiều target.
_cache: dict[str, dict] = {}

_NUMBERING_RE = re.compile(r'^\s*\d+[\.\)\-:]\s*')
_MAX_WORDS    = 7
_MAX_CHARS    = 90

# Hashtag quá chung → search ra hàng triệu video không liên quan.
# Loại thẳng kể cả khi Gemini đề xuất (đã gặp thực tế: viral, crime, cctv).
_TOO_GENERIC = frozenset({
    'viral', 'fyp', 'foryou', 'foryoupage', 'trending', 'video', 'videos',
    'news', 'crime', 'cctv', 'camera', 'police', 'shorts', 'reels', 'reel',
    'funny', 'wtf', 'omg', 'life', 'story', 'clip', 'tiktok', 'instagram',
    'accident', 'fight', 'theft', 'robbery', 'shooting', 'fire',
})


# ═══════════════════════════════════════════════════════════════════════════════
# Làm sạch & kiểm tra
# ═══════════════════════════════════════════════════════════════════════════════

def _clean(line: str) -> str:
    line = _NUMBERING_RE.sub('', line.strip())
    return line.strip(' "\'-•#\t')


def _valid_phrase(text: str) -> bool:
    """Loại dòng model dồn nhiều cụm dính vào nhau (thiếu ký tự xuống dòng)."""
    if not text or len(text) > _MAX_CHARS:
        return False
    words = text.split()
    if not words or len(words) > _MAX_WORDS:
        return False
    # Từ lặp trong cùng 1 dòng = dấu hiệu 2 cụm bị nối liền
    if len(set(w.lower() for w in words)) < len(words):
        return False
    return True


def to_hashtag(text: str) -> str | None:
    """
    'CCTV Trộm Cửa Hàng'  → 'cctvtromcuahang'
    'กล้องวงจรปิด ขโมย'    → 'กล้องวงจรปิดขโมย'   (giữ nguyên สระ tiếng Thái)

    Bỏ dấu với chữ LATIN vì hashtag có dấu trên TikTok/Instagram cho kết quả
    thất thường ('ộ' → 'o'). Nhưng KHÔNG bỏ dấu với hệ chữ khác: dấu trong
    tiếng Thái/Hindi/Ả Rập là thành phần bắt buộc của chữ, xóa đi thành từ
    khác hẳn hoặc vô nghĩa.
    """
    text = _clean(text).lower()
    if not text:
        return None

    # NFC trước: gom dấu Latin vào 1 ký tự để phân biệt Latin-có-dấu với
    # dấu độc lập của hệ chữ khác.
    text = unicodedata.normalize('NFC', text)

    out = []
    for ch in text:
        cat = unicodedata.category(ch)
        # L* = chữ, N* = số, Mn/Mc = dấu thanh (BẮT BUỘC giữ cho Thái/Devanagari)
        if not (cat[0] in 'LN' or cat in ('Mn', 'Mc')):
            continue                      # dấu cách, dấu câu, emoji → bỏ
        base = unicodedata.normalize('NFD', ch)[0]
        out.append(base if base.isascii() else ch)   # chỉ bỏ dấu khi là Latin

    tag = ''.join(out)
    if len(tag) < 5 or len(tag) > 40:
        return None
    if tag in _TOO_GENERIC:
        return None
    return tag


# ═══════════════════════════════════════════════════════════════════════════════
# Gọi Gemini
# ═══════════════════════════════════════════════════════════════════════════════

def _ask_gemini(prompt: str) -> str | None:
    try:
        from crawler_core.gemini_client import call_gemini
        return call_gemini(prompt)
    except Exception as e:
        logger.warning(f"[Keywords] Gemini thất bại: {e}")
        return None


def _random_language() -> str:
    try:
        from crawler_core.gemini_client import KEYWORD_LANGUAGES
        return random.choice(KEYWORD_LANGUAGES)
    except Exception:
        return 'Vietnamese'


def _label_block(active: list[str]) -> str:
    """Danh sách nhãn + mô tả, đưa vào prompt để Gemini không lẫn giữa các nhãn."""
    return '\n'.join(f"  {name} = {L.describe(name)}" for name in active)


def _parse_per_label(raw: str, active: list[str], as_hashtag: bool) -> dict[str, list[str]]:
    """
    Parse output dạng:
        Arson: cctv phóng hỏa | camera ghi lại đốt nhà | cctv arson footage
        Robbery: ...
    Nhãn nào thiếu/không hợp lệ sẽ được bù bằng keyword tĩnh ở hàm gọi.
    """
    # map cả tên nhãn và tên hiển thị, không phân biệt hoa thường
    lookup = {}
    for name in active:
        lookup[name.lower()] = name
        lookup[L.display(name).lower()] = name
        lookup[L.display(name).lower().replace(' ', '')] = name

    result: dict[str, list[str]] = {}
    rejected: list[str] = []

    for line in raw.splitlines():
        if ':' not in line:
            continue
        head, tail = line.split(':', 1)
        label = lookup.get(_clean(head).lower())
        if not label:
            continue

        items, seen = [], set()
        for piece in tail.split('|'):
            cleaned = _clean(piece)
            if not cleaned:
                continue
            if as_hashtag:
                value = to_hashtag(cleaned)
                if not value:
                    rejected.append(cleaned)
                    continue
            else:
                if not _valid_phrase(cleaned):
                    rejected.append(cleaned)
                    continue
                value = cleaned
            if value.lower() in seen:
                continue
            seen.add(value.lower())
            items.append(value)

        if items:
            result[label] = items[:PER_LABEL]

    if rejected:
        logger.info(
            f"[Keywords] Loại {len(rejected)} mục không hợp lệ/quá chung: "
            f"{rejected[:8]}{'...' if len(rejected) > 8 else ''}"
        )
    return result


def _generate(active: list[str], as_hashtag: bool) -> dict[str, list[str]]:
    language  = _random_language()
    n_local   = max(1, PER_LABEL // 2)
    n_english = max(1, PER_LABEL - n_local)

    if as_hashtag:
        form_rules = (
            f"Each item must be a HASHTAG: a single token with NO spaces "
            f"(join the words together), WITHOUT the '#' character, lowercase.\n"
            f"Make them SPECIFIC by combining a camera word with the incident "
            f"word, e.g. 'cctvarson', 'securitycamerashoplifting'.\n"
            f"Do NOT output broad tags like: viral, fyp, trending, crime, cctv, "
            f"camera, police, news.\n"
        )
        what = 'hashtags that TikTok and Instagram users put on such videos'
    else:
        form_rules = (
            f"Each item must be a short search query of 2-6 words, with normal "
            f"spaces between words.\n"
        )
        what = 'search queries people actually type to find such footage'

    prompt = (
        f"I am building a research dataset for video anomaly detection, similar "
        f"to the UCF-Crime benchmark. I need to find REAL footage recorded by "
        f"FIXED CCTV / security surveillance cameras.\n\n"
        f"These are the incident types I need, with what each one means:\n"
        f"{_label_block(active)}\n\n"
        f"For EACH incident type above, give me {PER_LABEL} {what}.\n"
        f"  - the first {n_local} item(s) written IN {language} (not English)\n"
        f"  - the remaining {n_english} item(s) written in English\n\n"
        f"{form_rules}\n"
        f"The items for one incident type must match THAT type specifically and "
        f"not the others — for example Stealing is stealthy theft without "
        f"confrontation, while Robbery uses force or threat against a victim, "
        f"and GunRobber / KnifeRobber are robberies where that weapon is "
        f"clearly visible.\n\n"
        f"Exclude live streams, movie scenes, news-anchor reports and "
        f"music-video compilations.\n\n"
        f"Output format — exactly one line per incident type, nothing else:\n"
        f"<IncidentType>: item1 | item2 | item3\n"
    )

    raw = _ask_gemini(prompt)
    if not raw:
        return {}

    parsed = _parse_per_label(raw, active, as_hashtag)
    kind = 'hashtag' if as_hashtag else 'cụm từ'
    logger.info(
        f"[Keywords] Gemini [{language}] sinh {kind} cho "
        f"{len(parsed)}/{len(active)} nhãn"
    )
    return parsed


# ═══════════════════════════════════════════════════════════════════════════════
# API dùng trong scraper — gọi TRONG task
# ═══════════════════════════════════════════════════════════════════════════════

def _by_label(kind: str, as_hashtag: bool,
              env_override: list[str] | None) -> dict[str | None, list[str]]:
    """
    Trả {nhãn: [keyword]}. Key None = không gán nhãn (khi override bằng env)
    → video vào thư mục mặc định thay vì thư mục nhãn.
    """
    if env_override:
        if as_hashtag:
            items = [t for t in (to_hashtag(x) for x in env_override) if t]
        else:
            items = [x for x in (_clean(x) for x in env_override) if x]
        logger.info(
            f"[Keywords] Dùng {len(items)} {kind} chỉ định qua env "
            f"(KHÔNG gán nhãn): {items}"
        )
        return {None: items}

    if kind in _cache:
        return _cache[kind]

    active = L.enabled_labels()
    generated = _generate(active, as_hashtag) if USE_LLM else {}

    # Bù nhãn nào Gemini không trả về (hoặc LLM bị tắt) bằng keyword tĩnh
    result: dict[str | None, list[str]] = {}
    fallback_labels = []
    for name in active:
        items = generated.get(name)
        if not items:
            static = L.static_keywords(name)
            items = (
                [t for t in (to_hashtag(s) for s in static) if t]
                if as_hashtag else list(static)
            )[:PER_LABEL]
            if items:
                fallback_labels.append(name)
        if items:
            result[name] = items

    if fallback_labels:
        reason = 'LLM tắt' if not USE_LLM else 'Gemini không trả kết quả hợp lệ'
        logger.info(
            f"[Keywords] {len(fallback_labels)} nhãn dùng {kind} tĩnh "
            f"({reason}): {fallback_labels}"
        )

    total = sum(len(v) for v in result.values())
    logger.info(f"[Keywords] Tổng {total} {kind} cho {len(result)} nhãn")
    _cache[kind] = result
    return result


def phrases_by_label(env_override: list[str] | None = None) -> dict[str | None, list[str]]:
    """Cụm từ search cho YouTube / Dailymotion / X / Reddit-search."""
    return _by_label('cụm từ', as_hashtag=False, env_override=env_override)


def hashtags_by_label(env_override: list[str] | None = None) -> dict[str | None, list[str]]:
    """Hashtag cho TikTok / Instagram."""
    return _by_label('hashtag', as_hashtag=True, env_override=env_override)
