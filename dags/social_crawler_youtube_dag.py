"""
Social CCTV Crawler — YouTube
================================
DAG riêng cho YouTube (tách từ social_cctv_video_crawler gộp trước đây) để
pause/xem lỗi/retry độc lập, không đụng tới các platform khác.

Logic crawl dùng chung — xem social_crawler_common.py.
Cấu hình từ khóa: platform_crawlers/config.py (CRAWL_YOUTUBE_KEYWORDS...)
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Airflow chỉ quét file có chứa từ "airflow" (DAG_DISCOVERY_SAFE_MODE, xem
# airflow/utils/file.py might_contain_dag_via_default_heuristic) — không có
# dòng này thì DagBag ÂM THẦM bỏ qua cả file, không báo lỗi.
import airflow  # noqa: F401

from dags.social_crawler_common import ROTATION_SCHEDULE, build_platform_dag

# Lịch = ROTATION_SCHEDULE cho cả 5 platform: pool 1 slot mới là thứ quyết định
# platform nào chạy khi nào — xem mục "Chạy TUẦN TỰ LIÊN TỤC" trong
# social_crawler_common.py.
# ⚠ YouTube là platform CHẬM NHẤT (~31 phút/nhãn × 11 nhãn ≈ 5.6h). Nếu không
# đặt CRAWL_TASK_TIME_BUDGET_MINUTES, nó sẽ giữ slot GPU hàng giờ và 4 platform
# còn lại gần như không được chạy.
dag = build_platform_dag('youtube', schedule=ROTATION_SCHEDULE)
