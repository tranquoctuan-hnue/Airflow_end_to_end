"""
Cấu hình discovery cho từng platform.

Mọi giá trị đều override được bằng env var (chuỗi phân tách bởi '|') nên
đổi keyword/hashtag không cần sửa code:

    export CRAWL_YOUTUBE_KEYWORDS="cctv trộm|cctv robbery"
    export CRAWL_TIKTOK_HASHTAGS="cctvfootage|camera247"
    export CRAWL_REDDIT_SUBS="CrimeCCTV|IdiotsInCars"
    export CRAWL_ENABLED_PLATFORMS="youtube|tiktok|reddit"
"""

import os


def _env_list(key: str, default: list[str]) -> list[str]:
    raw = os.environ.get(key, '').strip()
    if not raw:
        return default
    return [item.strip() for item in raw.split('|') if item.strip()]


def _env_list_or_none(key: str) -> list[str] | None:
    """
    None nếu env chưa set → để scraper tự sinh keyword bằng Gemini.
    Có set → dùng đúng list đó, KHÔNG gọi LLM.
    """
    raw = os.environ.get(key, '').strip()
    if not raw:
        return None
    return [item.strip() for item in raw.split('|') if item.strip()]


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, '').strip() or default)
    except ValueError:
        return default


def _env_bool(key: str, default: bool) -> bool:
    raw = os.environ.get(key, '').strip().lower()
    if not raw:
        return default
    return raw in ('1', 'true', 'yes', 'on')


# ── Platform nào được bật ────────────────────────────────────────────────────
# Task của platform không nằm trong danh sách này sẽ skip ngay (vẫn hiện trên UI).
ENABLED_PLATFORMS = _env_list(
    'CRAWL_ENABLED_PLATFORMS',
    ['youtube', 'tiktok', 'instagram', 'x', 'reddit', 'vimeo', 'dailymotion',
     'facebook'],
)

# ── Giới hạn ─────────────────────────────────────────────────────────────────
# Số URL lấy về cho mỗi keyword/hashtag/subreddit.
# ĐÂY LÀ NÚT ĐIỀU KHIỂN ĐỘ SÂU CUỘN TRANG. Vòng cuộn trong discovery.py dừng ở
# `if len(found) >= limit: break`, mà limit chính là biến này ⇒ tăng
# CRAWL_MAX_SCROLLS một mình KHÔNG có tác dụng gì, phải tăng cái này trước.
MAX_PER_TARGET = _env_int('CRAWL_MAX_PER_TARGET', 100)

# Số từ khóa Gemini sinh cho mỗi nhãn (đọc lại cùng env với keywords.PER_LABEL
# để hai chỗ không lệch nhau).
KEYWORDS_PER_LABEL = _env_int('CRAWL_KEYWORDS_PER_LABEL', 3)

# Trần MỖI NHÃN trong 1 lần chạy. Phải khớp với NĂNG LỰC THẬT của máy, không
# phải với số URL discovery lấy về được.
#
# Số học (đo 2026-08-05, GPU 6GB): VLM ~48s/URL ⇒ trần 60 phút
# (CRAWL_TASK_TIME_BUDGET_MINUTES) chỉ xử lý được ~75 URL/lượt.
#   25 URL/nhãn ≈ 20 phút ⇒ mỗi lượt đi qua ~3 nhãn ⇒ 11 nhãn trong ~4 lượt.
# Mỗi platform chạy 4-5 lượt/ngày nên cả 11 nhãn có dữ liệu trong ~1 ngày.
#
# ⚠ ĐẶT QUÁ CAO LÀ NGUYÊN NHÂN LÀM DATASET LỆCH, ngược với trực giác. Trước đó
# giá trị này là MAX_PER_TARGET × KEYWORDS_PER_LABEL = 300: không lượt nào xử lý
# nổi 300 URL trong 60 phút, mà `per_label` lại RESET mỗi lượt ⇒ nhãn không bao
# giờ đạt quota ⇒ con trỏ xoay vòng không bao giờ nhích. Kết quả đo được: reddit
# và youtube kẹt ở nhãn 'Abuse' suốt 5 ngày, 6/11 nhãn không có video nào.
#
# Trần này KHÔNG còn nhiệm vụ cân bằng nhãn (con trỏ xoay vòng lo việc đó) — nó
# chỉ còn nhiệm vụ chia thời gian của 1 lượt cho nhiều nhãn.
MAX_PER_LABEL = _env_int('CRAWL_MAX_PER_LABEL', 25)

# Trần tổng cho mỗi platform trong 1 LƯỢT chạy — chốt an toàn. Thực tế thứ chặn
# trước là CRAWL_TASK_TIME_BUDGET_MINUTES (mỗi lượt ~60 phút).
MAX_PER_PLATFORM = _env_int('CRAWL_MAX_PER_PLATFORM', 1200)

# ── Độ sâu discovery ─────────────────────────────────────────────────────────
# Số lần cuộn tối đa cho mỗi trang kết quả (X, Reddit tab Media, TikTok,
# Instagram). Vòng cuộn còn tự dừng khi 3 lần liên tiếp không ra link mới —
# tức là ĐÃ TỚI ĐÁY thật, không cần cuộn thêm.
MAX_SCROLLS = _env_int('CRAWL_MAX_SCROLLS', 60)

# Trần thời gian cho MỘT lượt discovery (1 từ khóa, 1 trang). Cuộn sâu tốn
# ~2.5s/lần nên 60 lần cuộn ≈ 150s + thời gian tải trang.
# ⚠ Quá hạn KHÔNG còn làm mất kết quả nữa: cả 3 hàm discovery đã sửa để trả về
# phần đã gom được (trước đây `return []` — mất trắng, xem discovery.py).
DISCOVERY_TIMEOUT = _env_int('CRAWL_DISCOVERY_TIMEOUT', 600)

# Kích thước batch để log tiến độ
BATCH_SIZE = _env_int('CRAWL_BATCH_SIZE', 25)

# Giới hạn thời lượng video (giây) trước khi tải — mặc định 1800s (30 phút)
# cho MỌI platform, override riêng theo platform nếu cần (không ảnh hưởng
# các platform khác). Dailymotion đặt 900s (15 phút): search của Dailymotion
# hay lẫn phim/show TV dài (đã gặp thực tế 32 phút) mà bucket "5-30 min" của
# chính Dailymotion không lọc được chính xác tới mức 15 phút — enforce cứng
# ở đây thay vì dựa vào filter URL không đủ chi tiết.
DEFAULT_MAX_DURATION_S = _env_int('CRAWL_MAX_DURATION', 1800)
PLATFORM_MAX_DURATION_S = {
    'dailymotion': _env_int('CRAWL_DAILYMOTION_MAX_DURATION', 900),
}


def max_duration_for(platform: str) -> int:
    return PLATFORM_MAX_DURATION_S.get(platform, DEFAULT_MAX_DURATION_S)

# Trần thời gian MỖI TASK (phút), 0 = không giới hạn (mặc định — chạy hết
# discovery mới dừng). Dùng cho smoke-test: task tự DỪNG ĐÚNG LỊCH giữa các
# batch/nguồn thay vì bị Airflow SIGKILL giữa chừng khi chạm execution_timeout
# — kill cứng có thể để lại file tải dở, tiến trình ffmpeg mồ côi.
# Đặt qua trigger conf {"time_budget_minutes": 8} cho 1 lần chạy thử, không
# cần sửa code hay DAG.
# Trần thời gian MỖI LƯỢT chạy của 1 platform, phút. Task tự dừng ở ranh giới an
# toàn (giữa 2 URL) rồi nhả slot GPU cho platform kế tiếp — xem mục "Chạy TUẦN TỰ
# LIÊN TỤC" trong dags/social_crawler_common.py.
#
# ⚠ Mặc định 60, KHÔNG phải 0. Trước đây mặc định 0 (= không giới hạn) và giá trị
# 60 chỉ được export trong start_airflow.sh — nên ai khởi động bằng
# `airflow standalone` trực tiếp thì trần này KHÔNG tồn tại. Hậu quả thật
# 2026-08-03: lượt dailymotion chạy hết 3h execution_timeout rồi bị kill
# (AirflowTaskTimeout), chiếm trọn slot GPU 3 tiếng và 4 platform kia không được
# chạy. Đặt mặc định ở đây để trần luôn có hiệu lực bất kể cách khởi động.
# 0 = không giới hạn (chỉ nên dùng khi chạy tay 1 platform).
TASK_TIME_BUDGET_MINUTES = _env_int('CRAWL_TASK_TIME_BUDGET_MINUTES', 60)

# ── Từ khóa search ───────────────────────────────────────────────────────────
# Để TRỐNG (mặc định) → scraper tự sinh bằng Gemini mỗi lần chạy task, ngôn ngữ
#   chọn ngẫu nhiên trong 21 ngôn ngữ (xem platform_crawlers/keywords.py).
#   Clip CCTV thường có caption tiếng bản địa nên đa ngôn ngữ tìm được nhiều hơn.
# Có set → dùng đúng list đó, không gọi LLM:
#   export CRAWL_YOUTUBE_KEYWORDS="cctv trộm|camera an ninh ghi lại"
#
# Tắt LLM hoàn toàn (dùng list tĩnh trong keywords.py): CRAWL_LLM_KEYWORDS=false
YOUTUBE_KEYWORDS_OVERRIDE     = _env_list_or_none('CRAWL_YOUTUBE_KEYWORDS')
DAILYMOTION_KEYWORDS_OVERRIDE = _env_list_or_none('CRAWL_DAILYMOTION_KEYWORDS')

# ── Dailymotion: dùng API công khai thay vì quét HTML ───────────────────────
# Trang search HTML của Dailymotion trần CỨNG 20 kết quả/từ khóa — đã kiểm chứng
# 2026-08-03: ?page=1/2/3 trả về ĐÚNG CÙNG 20 ID, không có link/nút phân trang,
# cuộn 12 lần không thêm ID nào. (Ghi chú "mỗi trang 20 kết quả, ?page=N để xem
# thêm" từ 2026-07-30 giờ đã SAI — Dailymotion đổi hành vi.)
#
# API công khai api.dailymotion.com/videos thì cho 100 kết quả/request,
# total=1000/query, KHÔNG cần cookie, và kết quả LIÊN QUAN HƠN. Đo thực tế:
#     HTML scrape : 20 URL / 8.5s   (cần cookie login, hay chết)
#     API         : 500 URL / 6.5s  (không cần cookie)
# Đặt false để quay về đường quét HTML cũ (vẫn giữ làm fallback tự động).
DAILYMOTION_USE_API = _env_bool('CRAWL_DAILYMOTION_USE_API', True)

# Trần số request API cho mỗi từ khóa (100 kết quả/request, API chặn ở total=1000
# nên >10 là vô nghĩa).
DAILYMOTION_API_MAX_PAGES = _env_int('CRAWL_DAILYMOTION_API_MAX_PAGES', 10)

# Số trang ?page=N cho đường quét HTML (chỉ còn dùng khi API lỗi). Giữ lại vì
# hàm tự dừng khi trang trùng trang trước nên để cao không tốn thêm gì.
DAILYMOTION_MAX_PAGES = _env_int('CRAWL_DAILYMOTION_MAX_PAGES', 15)

# ── Hashtag (TikTok / Instagram) ─────────────────────────────────────────────
# TikTok/Instagram search bằng trang /tag/<hashtag> nên PHẢI là 1 token không
# dấu cách — cụm từ có dấu cách sẽ ra 0 kết quả. Giá trị nhập tay vẫn được
# tự chuẩn hóa ("cctv trộm" → "cctvtrom").
TIKTOK_HASHTAGS_OVERRIDE    = _env_list_or_none('CRAWL_TIKTOK_HASHTAGS')
INSTAGRAM_HASHTAGS_OVERRIDE = _env_list_or_none('CRAWL_INSTAGRAM_HASHTAGS')

# ── Reddit ───────────────────────────────────────────────────────────────────
# Subreddit nhiều CLIP VIDEO. Lưu ý: r/CCTV chủ yếu là thảo luận mua/lắp
# thiết bị (bài text), đã kiểm tra thực tế → không đưa vào default.
REDDIT_SUBREDDITS = _env_list('CRAWL_REDDIT_SUBS', [
    'CaughtOnCamera', 'PublicFreakout', 'IdiotsInCars',
    'Whatcouldgowrong', 'instantkarma',
])
# Search toàn Reddit (không giới hạn subreddit) bằng keyword của từng nhãn.
# Rộng nhất và chỉ tốn 1 request/keyword. Tắt nếu chỉ muốn quét các sub trên.
REDDIT_GLOBAL_SEARCH = os.environ.get(
    'CRAWL_REDDIT_GLOBAL_SEARCH', 'true'
).lower() == 'true'

# Reddit đã CHẶN truy cập .json ẩn danh (HTTP 403 — đã test 4 cách đều chặn).
# Cần đăng nhập, theo thứ tự ưu tiên:
#
#   1. OAuth + tài khoản thật (TỐT NHẤT — thấy được bài NSFW):
#        Tạo app: https://www.reddit.com/prefs/apps → "create app" → type "script"
#        REDDIT_CLIENT_ID=<id ngay dưới tên app>
#        REDDIT_CLIENT_SECRET=<dòng secret>
#        REDDIT_USERNAME=<nick>
#        REDDIT_PASSWORD=<mật khẩu>        # có 2FA: "matkhau:OTP"
#      Clip CCTV/crime trên Reddit rất hay bị gắn NSFW → phải đăng nhập user
#      mới thấy. Nhớ bật "I am over 18" trong Settings của tài khoản.
#
#   2. OAuth app-only (chỉ CLIENT_ID + SECRET): chạy được nhưng bài NSFW bị ẩn.
#
#   3. Không có gì → fallback Playwright (dùng cookie nếu đã lưu bằng
#      scripts/import_browser_cookies.py reddit). Chậm hơn và không lọc được
#      bài nào có video trước khi tải.
REDDIT_HAS_OAUTH = bool(
    os.environ.get('REDDIT_CLIENT_ID', '').strip()
    and os.environ.get('REDDIT_CLIENT_SECRET', '').strip()
)
REDDIT_HAS_USER_LOGIN = REDDIT_HAS_OAUTH and bool(
    os.environ.get('REDDIT_USERNAME', '').strip()
    and os.environ.get('REDDIT_PASSWORD', '').strip()
)

# ── Facebook ─────────────────────────────────────────────────────────────────
# Discovery đi qua tab "Thước phim" của trang search (= /search/videos/, đã
# kiểm chứng bằng href của tab trên UI tiếng Việt). Bắt buộc cookie đăng nhập:
# thiếu cookie thì Playwright không mở nổi trang (ERR_CONNECTION_REFUSED),
# chứ không phải hiện trang login.
#
# Cookie: python scripts/import_browser_cookies.py facebook
#   → cookies/facebook_cookies.json  (Playwright, dùng cho discovery)
#   → cookies/facebook_cookies.txt   (Netscape, dùng cho lúc TẢI)
#
# ⚠ KHÔNG dùng cookies/facebook_legacy_cookies.txt cho DAG này: file đó bị
# qwen_classifier.py dùng làm cookiefile chung cho MỌI platform, và yt-dlp ghi
# ngược cả jar vào đó → đã thấy thực tế nó bị đè mất c_user/xs, chỉ còn cookie
# tracking lẫn với cookie của dailymotion/reddit/x/youtube.
FACEBOOK_KEYWORDS_OVERRIDE = _env_list_or_none('CRAWL_FACEBOOK_KEYWORDS')

# Số lần cuộn trang kết quả. Đo thực tế (query 'cctv cướp giật'):
#   1 lần → 14 ID | 4 → 35 | 8 → 63 | 12 → 91 ID unique, vẫn tăng đều.
# Mỗi lần cuộn tốn ~2.5s nên tăng số này là đánh đổi trực tiếp URL/thời gian.
# Có env riêng vì FB nặng hơn các platform khác (mỗi thẻ video nhiều DOM hơn).
FACEBOOK_MAX_SCROLLS = _env_int('CRAWL_FACEBOOK_MAX_SCROLLS', MAX_SCROLLS)

# ── Vimeo ────────────────────────────────────────────────────────────────────
# Vimeo KHÔNG có search extractor trong yt-dlp → phải chỉ định channel/user cụ thể.
# Dạng hợp lệ: https://vimeo.com/channels/<name>  |  https://vimeo.com/<user>
VIMEO_TARGETS = _env_list('CRAWL_VIMEO_TARGETS', [])

# ── X (Twitter) ──────────────────────────────────────────────────────────────
# yt-dlp không có extractor liệt kê timeline → discovery dùng Playwright và
# CẦN cookie đăng nhập. Query tự sinh giống YouTube, nhưng được nối thêm
# 'filter:videos' (cú pháp search của X) để chỉ lấy tweet có video.
X_QUERIES_OVERRIDE = _env_list_or_none('CRAWL_X_QUERIES')
X_SEARCH_SUFFIX    = os.environ.get('CRAWL_X_SEARCH_SUFFIX', 'filter:videos')
X_ACCOUNTS         = _env_list('CRAWL_X_ACCOUNTS', [])

# Tab của trang search X: 'media' (Phương tiện) | 'live' (Latest) | 'top'.
# Mặc định 'media' — đã đo thực tế 2026-07-31, cùng query, cùng cookie:
#   media → 100 tweet unique (chạm trần limit=100), 11s
#   live  →  49 tweet unique,                       32s
#   top   →  49 tweet unique,                       29s
# Media có riêng 75 tweet mà Latest không có (Latest có riêng 24). Đổi sang
# 'live' nếu cần ưu tiên bài MỚI NHẤT thay vì bài hợp nhất — Latest sắp theo
# thời gian, Media sắp theo độ liên quan.
X_SEARCH_TAB = os.environ.get('CRAWL_X_SEARCH_TAB', 'media').strip() or 'media'

# ── Portrait filter theo platform ────────────────────────────────────────────
# TikTok/Reels/Shorts gần như luôn 9:16. Nếu vẫn muốn giữ clip CCTV bị repost
# dạng dọc trên các platform đó, thêm tên platform vào đây.
ALLOW_PORTRAIT = _env_list('CRAWL_ALLOW_PORTRAIT', [])
