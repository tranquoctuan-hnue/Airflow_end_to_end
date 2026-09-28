"""
Sinh từ khóa search YouTube bằng Gemini (Google) — mỗi lần gọi chọn NGẪU NHIÊN 1
ngôn ngữ trong KEYWORD_LANGUAGES, để tăng khả năng tìm ra camera CCTV thật ở nhiều
nước (kênh camera địa phương thường đặt tiêu đề bằng tiếng bản địa).

Nếu USE_LLM_KEYWORDS=false, hoặc gọi Gemini thất bại (hết quota mọi key, lỗi mạng...),
hoặc không parse được dòng nào hợp lệ, sẽ dùng lại SEARCH_KEYWORDS tĩnh (tiếng Anh)
làm fallback.
"""

import random
import re
from typing import List

from src.modules.logger import default_logger as logger

from .config import KEYWORD_LANGUAGES, NUM_LLM_KEYWORDS, SEARCH_KEYWORDS, USE_LLM_KEYWORDS

_NUMBERING_RE = re.compile(r'^\s*\d+[\.\)\-:]\s*')

# Cụm từ search bình thường không dài/nhiều từ như vậy. Model đôi khi không xuống dòng
# đúng giữa các câu (đặc biệt vài ngôn ngữ) và dồn nhiều cụm thành 1 dòng duy nhất —
# dùng ngưỡng này để loại các dòng "dính" đó thay vì search bằng 1 câu vô nghĩa.
_MAX_KEYWORD_WORDS = 6
_MAX_KEYWORD_CHARS = 80


def _clean_line(line: str) -> str:
    line = _NUMBERING_RE.sub('', line.strip())
    return line.strip(' "\'-•\t')


def _is_valid_keyword(text: str) -> bool:
    if not text or len(text) > _MAX_KEYWORD_CHARS:
        return False
    words = text.split()
    if not words or len(words) > _MAX_KEYWORD_WORDS:
        return False
    # Từ lặp lại trong cùng 1 dòng là dấu hiệu model dồn nhiều cụm dính vào nhau
    # (ví dụ đã gặp: 2 cụm giống hệt nối liền nhau do thiếu xuống dòng).
    if len(set(words)) < len(words):
        return False
    return True


def generate_search_keywords(num_keywords: int = NUM_LLM_KEYWORDS) -> List[str]:
    """
    Chọn ngẫu nhiên 1 ngôn ngữ, nhờ Gemini sinh các cụm từ search YouTube bằng ngôn
    ngữ đó để tìm luồng CCTV/camera an ninh trực tiếp.
    Trả về SEARCH_KEYWORDS tĩnh nếu USE_LLM_KEYWORDS=false, Gemini lỗi, hoặc không
    parse được dòng nào hợp lệ từ output.
    """
    if not USE_LLM_KEYWORDS:
        return SEARCH_KEYWORDS

    language = random.choice(KEYWORD_LANGUAGES)
    prompt = (
        f"Generate {num_keywords} short YouTube search queries written IN {language} "
        f"(not English) that people would type to find LIVE CCTV / security camera / "
        f"traffic camera streams — real live surveillance footage, not documentaries "
        f"or compilations.\n"
        f"Reply with exactly {num_keywords} lines, one query per line, no numbering, "
        f"no quotes, no explanation."
    )

    try:
        from crawler_core.gemini_client import call_gemini
        raw = call_gemini(prompt)
    except Exception as e:
        logger.error(f"Sinh từ khóa bằng Gemini thất bại, dùng fallback tĩnh: {e}")
        return SEARCH_KEYWORDS

    seen = set()
    keywords = []
    skipped_invalid = 0
    for line in raw.splitlines():
        cleaned = _clean_line(line)
        if not cleaned or cleaned in seen:
            continue
        if not _is_valid_keyword(cleaned):
            skipped_invalid += 1
            continue
        seen.add(cleaned)
        keywords.append(cleaned)
        if len(keywords) >= num_keywords:
            break

    if not keywords:
        logger.warning(
            f"Không parse được từ khóa hợp lệ từ output Gemini ({language}, "
            f"{skipped_invalid} dòng bị loại vì quá dài/nhiều từ), dùng fallback tĩnh"
        )
        return SEARCH_KEYWORDS

    logger.info(f"Từ khóa search sinh bằng Gemini [{language}]: {keywords}")
    return keywords
