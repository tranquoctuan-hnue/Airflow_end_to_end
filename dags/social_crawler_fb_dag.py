"""
Social CCTV Crawler — Facebook (search theo từ khóa)
=====================================================
DAG riêng cho Facebook, đi theo LUỒNG SEARCH: mỗi từ khóa → mở tab
"Thước phim" của trang kết quả → cuộn để tải thêm → đẩy URL qua
crawler_core.pipeline.VideoPipeline (dedup 4 tầng dùng chung).

KHÁC với legacy_fb_groups_crawler_dag.py (đã có sẵn):
    legacy_fb_groups_crawler  quét VIDEO TRONG CÁC NHÓM đã tham gia
    social_crawler_fb         SEARCH theo từ khóa sinh từ 27 nhãn sự việc
Hai DAG bổ sung cho nhau, dùng chung DB + fingerprint store nên video trùng
giữa hai đường sẽ bị L1/L2/L3/L4 chặn, không tải hai lần.

"Thước phim" = tab /search/videos/ (đã kiểm chứng bằng href của tab trên UI
tiếng Việt: Tất cả→/search/top/, Mọi người→/search/people/,
**Thước phim→/search/videos/**, Trang→/search/pages/).

BẮT BUỘC cookie đăng nhập. Thiếu cookie thì KHÔNG phải bị đá về trang
login mà trang không tải được luôn: net::ERR_CONNECTION_REFUSED (đã tái
hiện 3/3 lần; cùng lúc đó chạy CÓ cookie vẫn ra kết quả bình thường).
    python scripts/import_browser_cookies.py facebook

⚠ Cookie dùng ở đây là cookies/facebook_cookies.{json,txt}, KHÔNG phải
cookies/facebook_legacy_cookies.txt (file đó bị qwen_classifier.py dùng chung
cho mọi platform và bị yt-dlp ghi đè, đã mất login thực tế).

execution_timeout đặt 4h thay vì 3h mặc định: discovery Facebook chạy bằng
Playwright + cuộn (~1 phút/từ khóa với 15 lần cuộn), và bước TẢI phải bắt
từng DASH segment qua Playwright nên chậm hơn yt-dlp đáng kể.

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
    'facebook',
    schedule=ROTATION_SCHEDULE,
    dag_id='social_crawler_fb',
    execution_timeout=timedelta(hours=4),
)
