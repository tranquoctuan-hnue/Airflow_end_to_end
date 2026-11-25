"""Cấu hình DAG camera_source_youtube_live: tìm luồng CCTV đang phát trực tiếp trên YouTube."""

import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Mọi giá trị dưới đây chỉnh được trong config/dags.yaml (phần camera_source_youtube_live)
# hoặc bằng biến môi trường YOUTUBE_CCTV_* cùng tên.


def _env_list(name, default):
    raw = os.environ.get(name, '').strip()
    return [x.strip() for x in raw.split('|') if x.strip()] if raw else default


def _env_int(name, default):
    try:
        return int(os.environ.get(name, '').strip() or default)
    except ValueError:
        return default


def _env_path(name, default):
    return os.environ.get(name, '').strip() or default

# Search keywords tĩnh — dùng làm FALLBACK khi USE_LLM_KEYWORDS=false hoặc khi sinh
# từ khóa bằng Gemini thất bại. Mỗi keyword được search riêng qua yt-dlp (ytsearchN:<keyword>).
SEARCH_KEYWORDS = _env_list("YOUTUBE_CCTV_KEYWORDS", [
    "live cctv camera",
    "live traffic camera",
    "live street camera",
    "live security camera",
    "live city cam",
])

# Sinh từ khóa search bằng Gemini (Google) thay vì SEARCH_KEYWORDS tĩnh ở trên.
# Tắt bằng env YOUTUBE_CCTV_LLM_KEYWORDS=false để luôn dùng SEARCH_KEYWORDS (không gọi API).
USE_LLM_KEYWORDS = os.environ.get("YOUTUBE_CCTV_LLM_KEYWORDS", "true").lower() == "true"

# Ngôn ngữ sinh từ khóa: dùng chung với crawler video (platform_crawlers/keywords.py)
from crawler_core.gemini_client import KEYWORD_LANGUAGES  # noqa: E402,F401

# Số từ khóa yêu cầu model sinh ra mỗi lần
NUM_LLM_KEYWORDS = _env_int("YOUTUBE_CCTV_NUM_LLM_KEYWORDS", 5)

# Số kết quả lấy về cho mỗi keyword (yt-dlp ytsearchN:)
RESULTS_PER_KEYWORD = _env_int("YOUTUBE_CCTV_RESULTS_PER_KEYWORD", 15)

# File txt lưu kết quả: mỗi dòng "<url>\t<image_path>\t<title>"
OUTPUT_TXT_PATH = _env_path("YOUTUBE_CCTV_OUTPUT_FILE", os.path.join(
    PROJECT_ROOT, "data", "videos", "youtube_cctv", "live_streams.txt"
))

# Thư mục lưu ảnh snapshot
SNAPSHOT_DIR = _env_path("YOUTUBE_CCTV_SNAPSHOT_DIR", os.path.join(
    PROJECT_ROOT, "data", "videos", "youtube_cctv", "snapshots"))

# Lịch sử MỌI link đã kiểm tra (dù kết quả là added / capture_failed / rejected_not_cctv):
# mỗi dòng "<url>\t<status>\t<timestamp>". Dùng để không kiểm tra lại 1 link nhiều lần
# (khác với OUTPUT_TXT_PATH — chỉ chứa link đã XÁC NHẬN là CCTV).
CHECKED_HISTORY_PATH = _env_path("YOUTUBE_CCTV_CHECKED_HISTORY", os.path.join(
    PROJECT_ROOT, "data", "videos", "youtube_cctv", "checked_history.txt"
))

# Xác minh lại bằng model thị giác (CCTVClassifier - Qwen2.5-VL local,
# crawler_core/qwen_classifier.py) trước khi ghi nhận 1 stream. Tắt bằng env YOUTUBE_CCTV_USE_CLASSIFIER=false
# nếu chỉ muốn lọc theo tiêu đề cho nhanh (không cần GPU).
USE_VISUAL_CLASSIFIER = os.environ.get("YOUTUBE_CCTV_USE_CLASSIFIER", "true").lower() == "true"
