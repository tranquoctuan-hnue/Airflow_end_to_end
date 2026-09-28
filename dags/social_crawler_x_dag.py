"""
Social CCTV Crawler — X (Twitter)
================================
DAG riêng cho X. Cần cookie đăng nhập (X không có extractor liệt kê
timeline trong yt-dlp → discovery luôn qua Playwright, cần session thật):

    python scripts/import_browser_cookies.py --list      # profile nào có cookie X
    python scripts/import_browser_cookies.py x           # profile Default
    python scripts/import_browser_cookies.py x --profile "Profile 1"

ĐỪNG hardcode profile: phiên X đã nhảy giữa Default và Profile 1 trong vòng
1 ngày (2026-07-31). Luôn --list trước.

CẢNH BÁO: cookie X có hạn tới 2027 nhưng phiên vẫn có thể bị server thu hồi —
lúc đó mọi search trả 0 URL mà task VẪN BÁO SUCCESS (unavailable_reason()
chỉ kiểm tra file cookie có tồn tại). Kiểm tra phiên bằng browser, KHÔNG
bằng API v1.1 (cho âm tính giả). Chi tiết: .claude/skills/x-dag/SKILL.md

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
dag = build_platform_dag('x', schedule=ROTATION_SCHEDULE)
