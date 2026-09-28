"""
Social CCTV Crawler — Dailymotion
================================
DAG riêng cho Dailymotion. Không cần cookie — extractor search của yt-dlp
đang hỏng (trả 0 entry không kèm lỗi) nên discovery tự fallback sang quét
HTML bằng Playwright (xem crawler_core... platform_crawlers/discovery.py:
discover_with_fallback), vẫn chạy được ẩn danh.

Logic crawl dùng chung — xem social_crawler_common.py.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Airflow chỉ quét file có chứa từ "airflow" (DAG_DISCOVERY_SAFE_MODE) —
# không có dòng này DagBag sẽ ÂM THẦM bỏ qua cả file, không báo lỗi.
import airflow  # noqa: F401

from dags.social_crawler_common import ROTATION_SCHEDULE, build_platform_dag

# Lịch = ROTATION_SCHEDULE cho cả 5 platform: pool 1 slot mới là thứ quyết định
# platform nào chạy khi nào — xem mục "Chạy TUẦN TỰ LIÊN TỤC" trong
# social_crawler_common.py.
dag = build_platform_dag('dailymotion', schedule=ROTATION_SCHEDULE)
