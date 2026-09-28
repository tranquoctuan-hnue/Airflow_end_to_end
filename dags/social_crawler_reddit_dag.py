"""
Social CCTV Crawler — Reddit
================================
DAG riêng cho Reddit.

execution_timeout đặt DÀI HƠN các platform khác (4h thay vì 3h mặc định):
Reddit đã chặn mọi truy cập .json ẩn danh (HTTP 403, đã kiểm chứng thực tế).
Nếu chưa cấu hình OAuth, discovery fallback sang Playwright cho TOÀN BỘ
11 nhãn × 3 keyword × (1 search toàn Reddit + 5 subreddit) = ~198 lượt mở
browser — có thể tốn 60-90+ phút chỉ riêng bước discovery.

Cấu hình OAuth (khuyến nghị — nhanh hơn nhiều, và cần thiết để thấy bài
NSFW mà clip CCTV/crime hay bị gắn) — thêm vào .env:
    REDDIT_CLIENT_ID=<id ngay dưới tên app tại reddit.com/prefs/apps>
    REDDIT_CLIENT_SECRET=<dòng secret>
    REDDIT_USERNAME=<nick>
    REDDIT_PASSWORD=<mật khẩu>          # có 2FA: "matkhau:OTP"
Sau khi cấu hình OAuth, có thể hạ execution_timeout xuống 3h như các
platform khác (sửa tham số execution_timeout bên dưới).

Logic crawl dùng chung — xem social_crawler_common.py.
"""

import os
import sys
from datetime import timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Airflow chỉ quét file có chứa từ "airflow" (DAG_DISCOVERY_SAFE_MODE) —
# không có dòng này DagBag sẽ ÂM THẦM bỏ qua cả file, không báo lỗi.
import airflow  # noqa: F401

from dags.social_crawler_common import ROTATION_SCHEDULE, build_platform_dag

# Lịch = ROTATION_SCHEDULE cho cả 5 platform: pool 1 slot mới là thứ quyết định
# platform nào chạy khi nào — xem mục "Chạy TUẦN TỰ LIÊN TỤC" trong
# social_crawler_common.py.
dag = build_platform_dag(
    'reddit',
    schedule=ROTATION_SCHEDULE,
)
