"""
Gọi Gemini (Google) để sinh text — dùng chung cho sinh từ khóa của crawler video
(platform_crawlers/keywords.py) và DAG YouTube live (youtube_live/keyword_generator.py).

Quota Gemini tính riêng theo từng (key, model) — 1 key hết quota ở model A không có
nghĩa các model khác của CÙNG key đó cũng hết. Nên chiến lược thử là:
  với mỗi key (thứ tự ngẫu nhiên):
    thử lần lượt GEMINI_MODEL rồi tới các GEMINI_MODEL_FALLBACKS (cùng key này)
    -> có 1 cặp (key, model) thành công là trả về ngay
  hết cả model cho key này mới chuyển sang key tiếp theo.
Raise RuntimeError nếu tất cả (key, model) đều thất bại.
"""

import json
import os
import random
from typing import List, Optional

from src.modules.logger import default_logger as logger

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# File chứa danh sách Gemini API key — tự xoay vòng qua các key khi 1 key bị lỗi/hết
# quota free-tier. Không commit (mẫu: config/gemini_keys.example.json).
GEMINI_KEYS_PATH = os.environ.get("GEMINI_KEYS_FILE") or os.path.join(
    PROJECT_ROOT, "config", "gemini_keys.json")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")

# Nếu GEMINI_MODEL bị rate-limit (429) trên 1 key, thử lần lượt các model dự phòng này
# VỚI CÙNG KEY đó trước khi chuyển sang key khác.
#
# Đã test thực tế (2026-07-24) toàn bộ 10 key x các model hỗ trợ generateContent:
#   - Dòng "pro" (gemini-2.5-pro, gemini-3-pro-preview, gemini-3.1-pro-preview,
#     gemini-pro-latest) và dòng "2.0" (gemini-2.0-flash, gemini-2.0-flash-lite) đều bị
#     429 RESOURCE_EXHAUSTED (limit: 0) trên CẢ 10/10 key — đây là giới hạn free-tier
#     vĩnh viễn (không phải hết quota tạm thời), nên KHÔNG đưa vào danh sách dự phòng
#     vì chắc chắn sẽ luôn thất bại, chỉ tốn thêm 1 lần gọi API vô ích.
#   - "gemini-2.5-flash-lite" bị 404 (không còn cho user mới).
#   - Chỉ dòng "flash"/"flash-lite"/"gemma" mới gọi được (OK trên cả 10/10 key):
#     gemini-2.5-flash, gemini-flash-latest, gemini-flash-lite-latest,
#     gemini-3-flash-preview, gemini-3.1-flash-lite(-preview), gemini-3.5-flash(-lite),
#     gemini-3.6-flash, gemma-4-26b-a4b-it, gemma-4-31b-it.
GEMINI_MODEL_FALLBACKS = [
    "gemini-flash-latest",
    "gemini-3.6-flash",
    "gemini-3.5-flash",
    "gemini-3-flash-preview",
    "gemini-flash-lite-latest",
    "gemini-3.1-flash-lite",
    "gemini-3.5-flash-lite",
    "gemma-4-31b-it",
]

# Mỗi lần sinh từ khóa, chọn NGẪU NHIÊN 1 ngôn ngữ trong danh sách này — tăng khả năng
# tìm ra camera CCTV thật ở nhiều nước khác nhau (kênh camera địa phương thường đặt tiêu
# đề bằng tiếng bản địa, khác với luôn search bằng tiếng Anh).
KEYWORD_LANGUAGES = [
    "Thai", "Chinese", "Persian (Farsi)", "Vietnamese", "Korean", "Japanese",
    "Arabic", "Russian", "Hindi", "Indonesian", "Turkish", "Spanish",
    "Portuguese", "French", "German", "Italian", "Filipino (Tagalog)",
    "Malay", "Burmese", "Khmer", "Polish",
]

_keys_cache: Optional[List[str]] = None


def _load_keys() -> List[str]:
    global _keys_cache
    if _keys_cache is None:
        with open(GEMINI_KEYS_PATH, "r", encoding="utf-8") as f:
            _keys_cache = json.load(f).get("keys", [])
    return _keys_cache


def call_gemini(prompt: str, model: str = GEMINI_MODEL) -> str:
    """
    Gọi Gemini với prompt text-only. Trả về text ngay khi có 1 cặp (key, model)
    thành công. Raise RuntimeError nếu mọi key x mọi model đều thất bại.
    """
    from google import genai

    keys = _load_keys()
    if not keys:
        raise RuntimeError(f"Không có Gemini API key nào trong {GEMINI_KEYS_PATH}")

    # model chính thử trước, rồi tới các model dự phòng (bỏ trùng nếu model chính
    # đã nằm trong danh sách fallback)
    models_to_try = [model] + [m for m in GEMINI_MODEL_FALLBACKS if m != model]

    shuffled_keys = keys[:]
    random.shuffle(shuffled_keys)

    last_error = None
    for key in shuffled_keys:
        client = genai.Client(api_key=key)
        key_suffix = key[-4:] if len(key) >= 4 else key
        for m in models_to_try:
            try:
                response = client.models.generate_content(model=m, contents=prompt)
                return response.text
            except Exception as e:
                last_error = e
                logger.warning(
                    f"Gemini lỗi (key ...{key_suffix}, model={m}): "
                    f"{type(e).__name__}: {e} — thử tiếp"
                )
                continue

    raise RuntimeError(
        f"Tất cả {len(keys)} key x {len(models_to_try)} model đều thất bại: {last_error}"
    )
