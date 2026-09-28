---
name: x-dag
description: Debug, vận hành và bảo trì DAG social_crawler_x (X/Twitter) — phiên có thể chết ở server dù cookie ghi hạn 2027 (API v1.1 kiểm tra cho âm tính giả, phải thử bằng browser), search trả 0 URL mà task vẫn báo SUCCESS, tab Media vs Latest, lọc 9:16 loại 50% video đã tải. Dùng khi DAG này ra 0 URL, downloaded=0, nghi cookie X hỏng, hoặc cần đăng nhập/nhập lại cookie X.
---

# social_crawler_x — Runbook

> **Cập nhật 2026-09-28: runbook này viết trước các thay đổi dưới đây. Khi mâu thuẫn, tin mục này.**
> - **Thứ tự lọc mới:** L0 → L1 → **tải về** → 9:16 → L2 → L3 → L4 → CLIP (CCTV) → VideoMAE (sự việc).
>   Video bị loại được chuyển sang `video_rejected/<lý do>/<nhãn>/` kèm `.json`, lý do ghi ở
>   `video_urls.reject_reason`. Xem `crawler_core/pipeline.py`.
> - **Qwen2.5-VL không còn là bộ lọc mặc định.** Thay bằng CLIP + OCR (`crawler_core/cctv_detector.py`)
>   và VideoMAE (`crawler_core/videomae_filter.py`); `CRAWL_CCTV_CLASSIFIER=qwen` để quay lại.
> - **Lịch chạy:** không còn "mỗi 10 phút, pool chọn ai chạy". Mặc định là **vòng xoay nối tiếp**
>   theo Variable `crawler_rotation` (`CRAWL_ROTATION_MODE=chain`); lịch `@daily` chỉ để có Backfill.
> - **27 nhãn** (không còn 11): FallOver gộp vào Collapse, Shoplifting gộp vào Stealing. Xem skill `manage-labels`.
> - Mỗi bộ lọc **bật/tắt được theo platform** trên form Trigger / Variable `crawler_filters`.
>   Chạy thử bằng **Chế độ test**, xem skill `test-pipeline`.

DAG crawl video CCTV từ X (Twitter), đi qua cổng dedup 4 tầng dùng chung
(`crawler_core.pipeline.VideoPipeline`). File chính:

| File | Vai trò |
|---|---|
| `dags/social_crawler_x_dag.py` | Định nghĩa DAG — schedule `ROTATION_SCHEDULE` (10 phút, xoay vòng liên tục qua pool 1 slot — xem mục "Chạy TUẦN TỰ LIÊN TỤC" trong `social_crawler_common.py`), KHÔNG override gì nên dùng mặc định: `execution_timeout=3h`, `retries=1`, `retry_delay=15m` |
| `dags/social_crawler_common.py` | `task_crawl_platform()` — logic chạy chung mọi platform |
| `platform_crawlers/scrapers.py` → `XScraper` | Sinh query theo nhãn, nối `filter:videos`, gọi Playwright. `needs_cookies = True` |
| `platform_crawlers/discovery.py` → `playwright_discover()` | Mở trang, cuộn, gom href khớp `/status/\d+`. X KHÔNG có hàm discovery riêng như Reddit/Dailymotion |
| `platform_crawlers/config.py` | `X_QUERIES_OVERRIDE`, `X_SEARCH_SUFFIX` (`filter:videos`), `X_ACCOUNTS` (mặc định RỖNG) |
| `crawler_core/dedup.py` → `_PLATFORM_RULES['x']` | Canonical ID (L1): `/i/status/<id>`, `/status/<id>`. Host: `x.com`, `twitter.com`, `mobile.*` |
| `crawler_core/downloader.py` → `_COOKIE_PATHS['x']` | `cookies/x_cookies.txt` cho yt-dlp lúc TẢI |
| `cookies/x_cookies.json` / `.txt` | Cookie đăng nhập — `.json` cho Playwright (discovery), `.txt` cho yt-dlp (download) |
| `scripts/import_browser_cookies.py x --profile "Profile 1"` | Cách lấy cookie ĐÚNG cho máy này — xem mục 1.5 |

---

## 0. Trạng thái đã kiểm chứng (2026-07-31) — đọc trước khi debug

| Hạng mục | Trạng thái thực tế | Hệ quả |
|---|---|---|
| DAG `social_crawler_x` | **đang PAUSE** (`is_paused=1`), lịch 03:00 | Chưa từng có run riêng — chỉ có log từ DAG cũ `social_cctv_video_crawler` |
| Phiên đăng nhập X | **SỐNG** (chiều 2026-07-31, user `@Tuanzeda`) sau khi đăng nhập lại. Trước đó CHẾT — xem 1.1b | Search ra kết quả bình thường trở lại |
| Cookie file | 22 cookie, nhập từ **Chrome profile Default** (KHÔNG phải Profile 1 — profile đó vẫn giữ token chết) | Xem mục 1.5 — phải kiểm tra cả 2 profile |
| Tab search | **`f=media`** (Phương tiện) từ 2026-07-31, trước đó `f=live` | Gấp đôi số tweet, nhanh gấp 3 — xem mục 1.7 |
| Kết quả trong DB | `sources`=0, `video_urls`=**0 dòng**, `fingerprints`=0 | X chưa bao giờ crawl được gì. Không có dữ liệu cũ nào cần dọn |

Bằng chứng từ lần chạy thật duy nhất (DAG cũ, task `crawl_x`,
run `scheduled__2026-07-29T02:00:00+00:00`, 10:17:06→10:21:12 UTC):

```
[Discovery] https://x.com/search?q=cctv%20robbery...filter%3Avideos&f=live → 0 URL
[x] x:Abuse:cctv caregiver abuse caught: không tìm được URL nào
... (19 lượt search trong log, TẤT CẢ đều 0 URL — task bị dừng từ bên
    ngoài lúc 10:21 nên chưa chạy hết 33 lượt)
```

**Việc phải làm đầu tiên:** đăng nhập lại X (mục 1.1). Chừng nào
`verify_credentials.json` chưa trả 200 thì mọi thứ khác đều vô nghĩa —
đừng sửa selector, đừng chỉnh thời gian chờ, đừng đổi headless.

---

## 1. Quirks đặc thù của X (đã kiểm chứng thực tế)

### 1.1 — Kiểm tra phiên X: CHỈ phép thử chức năng mới đáng tin
⚠ **Sửa một sai lầm của chính runbook này (bản sáng 2026-07-31).** Bản đầu
khuyên dùng `api.x.com/1.1/account/verify_credentials.json` và gọi đó là
"cách DUY NHẤT đáng tin". **Sai.** Đo lại chiều cùng ngày với phiên ĐANG SỐNG
(đã xác nhận sống bằng browser): v1.1 vẫn trả **401 code 32**, và một lần
khác trả **404 code 34** cho cùng bộ cookie. X đã bỏ xác thực v1.1 bằng
cookie+bearer web, nên endpoint này cho **âm tính giả** — báo "chết" khi
phiên vẫn sống. Đừng dùng nó để kết luận.

**Phép thử ĐÚNG — làm đúng việc mà crawler làm:**
```bash
python -c "
import json, re
from playwright.sync_api import sync_playwright
UA = ('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36')
cookies = json.load(open('cookies/x_cookies.json'))
with sync_playwright() as pw:
    b = pw.chromium.launch(headless=True, args=['--no-sandbox'])
    ctx = b.new_context(user_agent=UA, viewport={'width':1280,'height':900})
    ctx.add_cookies(cookies)
    p = ctx.new_page()
    p.goto('https://x.com/search?q=cctv%20robbery%20filter%3Avideos&f=media',
           wait_until='domcontentloaded', timeout=45000)
    p.wait_for_timeout(9000)
    hrefs = [a.get_attribute('href') or '' for a in p.query_selector_all('a[href]')]
    n = len({m.group(1) for h in hrefs if (m := re.search(r'/status/(\\d+)', h))})
    print('URL cuoi    :', p.url[:90])
    print('bi da login :', 'login' in p.url or 'onboarding' in p.url)
    print('tweet unique:', n)
    b.close()"
```
| Kết quả | Nghĩa là |
|---|---|
| `bi da login = False` + tweet > 0 | **Phiên SỐNG.** Lỗi (nếu có) nằm chỗ khác |
| URL cuối chứa `/i/jf/onboarding/web` hoặc `mode=login`, tweet = 0 | **Phiên CHẾT** → đăng nhập lại |

### 1.1b — Cookie "còn hạn tới 2027" không chứng minh phiên còn sống
Bẫy này vẫn đúng và vẫn nguy hiểm — chỉ là cách kiểm tra thì khác (mục 1.1):

| Platform | Cách phiên chết | Nhìn `expires` có biết không? |
|---|---|---|
| Reddit | cookie hết hạn theo `expires` | **Có** |
| **X** | server thu hồi phiên, cookie vẫn nằm đó hạn 2027 | **KHÔNG** |

Đã gặp thật sáng 2026-07-31: `auth_token` hạn `2027-03-11` (còn 8 tháng), đủ
`ct0`/`twid`/`kdt`/`guest_id`, giá trị GIỐNG HỆT (hash SHA-256) cookie sống
trong Chrome — mà browser vẫn bị đá sang trang đăng nhập. Nhập lại cookie
không cứu được; phải đăng nhập lại bằng tay trong Chrome.

**Khắc phục:**
```bash
# 1. Đăng nhập x.com bằng tay trong Chrome (profile nào cũng được — bước 2
#    sẽ tự tìm, xem mục 1.5). KHÔNG dùng Playwright: trang login X có
#    "Continue with Google" và Google chặn browser bị điều khiển.
# 2. Nhập cookie từ đúng profile vừa đăng nhập:
python scripts/import_browser_cookies.py x                      # profile Default
python scripts/import_browser_cookies.py x --profile "Profile 1"
# 3. Chạy phép thử chức năng ở mục 1.1 — phải thấy bi da login = False.
```

### 1.2 — `unavailable_reason()` chỉ kiểm tra FILE CÓ TỒN TẠI, không kiểm tra phiên
```python
# platform_crawlers/scrapers.py — XScraper
def unavailable_reason(self):
    if not cookies_json_for(self.platform):   # <-- chỉ os.path.exists()
        return 'X yêu cầu đăng nhập để search...'
    return None
```
Cookie chết nhưng file vẫn nằm đó ⇒ `unavailable_reason()` trả `None` ⇒ task
coi như SẴN SÀNG, chạy hết 33 lượt search, thu 0 URL, rồi **kết thúc ở trạng
thái SUCCESS**. Không có cảnh báo nào, không retry, Airflow UI xanh lè.

⇒ **DAG xanh KHÔNG có nghĩa là X đang hoạt động.** Với X phải nhìn
`downloaded=` trong log tổng kết, hoặc query DB (mục 3). Đã kiểm chứng
`get_scraper('x').unavailable_reason()` hiện trả `None` dù phiên chết.

### 1.3 — Triệu chứng khi phiên chết: redirect sang onboarding, 0 link `/status/`
Mở đúng URL mà scraper dùng, kèm cookie hiện tại, kết quả:
```
URL cuối : https://x.com/i/jf/onboarding/web?redirect_after_login=%2Fsearch%3F...&mode=login
title    : X - The Everything App / X
tổng <a> : 22          ← toàn link footer: /tos, /privacy, help.x.com, grok.com
khớp /status/\d+ : 0
body     : "See what's happening | Select an option below: | Continue with phone |
            Continue with Google | ... | Email or username | Continue"
```
Thấy `/i/jf/onboarding/web` hoặc `mode=login` trong URL cuối ⇒ chắc chắn là
phiên chết, không phải lỗi selector/timing.

Ghi chú: `/home` KHÔNG bị đá thẳng sang `mode=login` mà âm thầm redirect về
`https://x.com/` — nhìn qua tưởng bình thường. Đừng dùng `/home` làm phép thử
đăng nhập; dùng `verify_credentials.json` ở mục 1.1.

### 1.4 — KHÔNG phải bot-detection: đã thử 3 cách, hỏng như nhau
Trước khi ai đó đi sửa `headless`, thêm stealth plugin, hay đổi User-Agent:
đã đo cùng URL, cùng cookie, 2026-07-31 —

| Cách chạy | Link `/status/` tìm được | Bị đá login? |
|---|---|---|
| Chromium `headless=True` | 0 | **Có** |
| Chromium `headless=False` (có màn hình, `DISPLAY=:0`) | 0 | **Có** |
| Chrome thật (`channel='chrome'`, `headless=False`) | 0 | **Có** |

Cả ba redirect y hệt nhau sang `onboarding/web?...&mode=login`. Vấn đề nằm ở
**phiên**, không phải ở cách điều khiển browser. Đây là điểm khác Dailymotion
(nơi mẹo browser thật sự có tác dụng) — đừng mang kinh nghiệm đó sang đây.

Cookie CÓ vào được context (kiểm chứng bằng `ctx.cookies('https://x.com')` →
thấy đủ `auth_token, ct0, guest_id, kdt, twid`), nên cũng không phải lỗi
`add_cookies` hay sai domain.

### 1.5 — Cookie X nằm ở profile nào thì KHÔNG cố định — phải quét lại
Máy này có nhiều profile Chrome và phiên X **đã nhảy profile** trong vòng 1
ngày, nên đừng hardcode:

| Thời điểm | Default | Profile 1 |
|---|---|---|
| sáng 2026-07-31 | 5 cookie, chưa đăng nhập | 15 cookie, có `auth_token` (nhưng phiên đã chết) |
| chiều 2026-07-31 (sau khi user đăng nhập lại) | **22 cookie, phiên SỐNG** | 14 cookie, token cũ vẫn chết |

⚠ Docstring trong `dags/social_crawler_x_dag.py` khuyên
`--profile "Profile 1"` — đúng lúc viết, **sai từ chiều 2026-07-31**. Luôn
quét trước khi nhập:
```bash
python scripts/import_browser_cookies.py --list
```
Nhưng nhớ: `--list` chỉ so TÊN cookie, không biết hạn cũng không biết phiên
còn sống (nó báo `✓ đã login` cho cả profile có token chết). Nó chỉ dùng để
biết profile nào CÓ cookie; xác nhận sống/chết vẫn phải bằng mục 1.1.

Chạy `import_browser_cookies.py x` không kèm `--profile` sẽ đọc Default và
**bỏ qua im lặng** nếu profile đó chưa đăng nhập — file cũ giữ nguyên, dễ
tưởng đã nhập xong.

### 1.6 — `CRAWL_X_QUERIES` có HAI tác dụng phụ, không chỉ đổi từ khóa
```python
q = kw if cfg.X_QUERIES_OVERRIDE else f'{kw}{suffix}'
by_label = keywords.phrases_by_label(cfg.X_QUERIES_OVERRIDE)
```
Đặt `CRAWL_X_QUERIES="..."` sẽ đồng thời:
1. **Mất `filter:videos`** — suffix KHÔNG được nối vào query tự chỉ định, nên
   search ra cả tweet chữ/ảnh.
2. **Mất nhãn** — `phrases_by_label()` với override trả `{None: [...]}`, video
   rơi vào thư mục mặc định `data/videos/CCTV/` thay vì thư mục theo nhãn.

Muốn giữ cả hai thì phải tự viết `filter:videos` vào từng query, và chấp nhận
là không gán nhãn được:
```bash
export CRAWL_X_QUERIES="cctv robbery filter:videos|cctv arson filter:videos"
```

### 1.7 — Tab Media (`f=media`) thắng Latest rõ rệt — ĐÃ ĐO, đã đổi trong code
Giả thuyết ở bản runbook sáng nay đã được kiểm chứng chiều 2026-07-31, cùng
query `cctv robbery filter:videos`, cùng cookie, `limit=100`:

| Tab | URL param | Tweet unique | Thời gian |
|---|---|---|---|
| **Media (Phương tiện)** | `f=media` | **100** (chạm trần limit ⇒ còn nhiều hơn) | **11s** |
| Latest | `f=live` | 49 | 32s |
| Top | (không có `f=`) | 49 | 29s |

- Media có riêng **75** tweet mà Latest không có; Latest có riêng 24.
- Media nhanh gấp ~3 vì bố cục lưới → mỗi lần cuộn ra nhiều link hơn hẳn.
- Hợp cả 3 tab: 132 tweet unique.

Bài học từ Reddit (tab Media > API search) **lặp lại đúng ở X**. Đã đổi:
```python
# platform_crawlers/config.py
X_SEARCH_TAB = os.environ.get('CRAWL_X_SEARCH_TAB', 'media')
# platform_crawlers/scrapers.py — XScraper.discover()
f'https://x.com/search?q={quote(q)}&f={cfg.X_SEARCH_TAB}'
```
Đổi lại khi cần ưu tiên bài MỚI NHẤT thay vì hợp nhất (Latest sắp theo thời
gian, Media sắp theo độ liên quan): `export CRAWL_X_SEARCH_TAB=live`.

**Chất lượng URL thu được** — lấy 12 URL từ tab Media rồi kiểm bằng yt-dlp
(chỉ đọc metadata): **6/6 URL kiểm tra đều là video tải được**, thời lượng
41-142s. Kết hợp `filter:videos` trong query + tab Media cho tỉ lệ video rất
cao, nên X KHÔNG cần bước lọc metadata riêng như Reddit (`_reddit_keep_videos`).

Đo end-to-end qua `XScraper.discover()` thật: 40 URL/nguồn, 3 nguồn trong
**16s** (~5s/nguồn) — so với ~32s/nguồn của `f=live` trước đây.

### 1.7b — Filter People/Location: mặc định đã rộng nhất, GHI RÕ RA SẼ PHÁ
Panel **Filter** trên trang search X có 2 nhóm ngoài phần thời gian:

| Nhóm | Lựa chọn | Tham số URL |
|---|---|---|
| People | **From anyone** (mặc định) / People you follow | không có / `pf=on` |
| Location | **Anywhere** (mặc định) / Near you | không có / `lf=on` |

Cả hai lựa chọn RỘNG NHẤT đều được biểu đạt bằng cách **KHÔNG có tham số**.
X **không nhận giá trị `off`** — đo thực tế 2026-07-31, cùng query
`cctv robbery filter:videos&f=media`:

| URL | Tweet | Trang hiển thị |
|---|---|---|
| (không `pf`/`lf`) — code đang dùng | **100** | bình thường |
| `&pf=off&lf=off` | **0** | "No results for… Try searching for something else" |
| `&pf=off` | **0** | "Something went wrong" |
| `&lf=on` (Near you) | 58 | bình thường — xác nhận `lf` là filter thật |

⇒ **Đừng "sửa cho tường minh" bằng cách thêm `pf=off&lf=off`** — trông có vẻ
an toàn hơn nhưng thực chất làm search trả về rỗng. Đã có cảnh báo ngay tại
chỗ trong `XScraper.discover()`.

Xác nhận "From anyone" đang có hiệu lực thật (không phải X nhớ filter cũ của
tài khoản): tweet thu được đến từ @Abramjee, @CCTV_VIEWS, @CaughtCam404,
@dekingdomofnews… toàn tài khoản lạ, không phải người đang theo dõi.

### 1.7c — X THROTTLE search sau ~12 lượt liên tiếp, và thất bại IM LẶNG
Phát hiện 2026-07-31 khi đo A/B: sau khoảng **12 lượt search Playwright dồn
dập trong ~15 phút**, X bắt đầu trả trang rỗng. Nguy hiểm ở chỗ nó KHÔNG
giống lỗi đăng nhập chút nào:

```
URL cuối : https://x.com/search?q=...&f=media     ← đúng URL, KHÔNG bị đá login
sidebar  : Home | Explore | ... | Tuan | @Tuanzeda ← VẪN ĐANG ĐĂNG NHẬP
vùng kết quả: "Something went wrong. Try reloading." + nút "Retry"
tweet tìm được: 0
```

⇒ Nhìn từ log Airflow thì y hệt "từ khóa này không có kết quả" — cùng một
dòng `→ 0 URL`. **Không phân biệt được nếu chỉ đọc log.**

**Tự hồi phục:** đã kiểm chứng — nghỉ ~2 phút rồi thử lại là ra 45 tweet
bình thường, không cần đăng nhập lại, không cần đổi cookie.

**Vì sao đáng lo với DAG này:** 1 run có **33 lượt search** chạy liên tiếp
không nghỉ. Sau khi đổi sang tab Media, mỗi lượt chỉ còn ~5s (trước ~32s) —
tức là **bắn nhanh hơn 6 lần**, dồn 33 request vào ~3 phút, gần như chắc
chắn chạm throttle giữa chừng. Nghịch lý: tối ưu tốc độ lại làm tăng rủi ro.

**Cách phân biệt throttle với "hết nội dung thật"** — phải nhìn nội dung
trang, không nhìn số URL:
```python
body = page.inner_text('body')
throttled = 'Something went wrong' in body    # + thường có 'Retry'/'Try reloading'
```

**Chưa xử lý trong code.** Hai hướng, chưa chọn:
1. Nghỉ giữa các lượt search (ví dụ 8-10s) → 33 lượt mất thêm ~5 phút, vẫn
   nhanh hơn `f=live` cũ rất nhiều.
2. Phát hiện marker "Something went wrong" rồi backoff + thử lại, và log
   cảnh báo RIÊNG để không lẫn với "0 kết quả thật".
Hướng 2 tốt hơn nhưng phải sửa `playwright_discover()` — hàm DÙNG CHUNG cho
TikTok/Instagram/Reddit/X, nên cần thêm tham số marker thay vì hardcode.

### 1.8 — Lọc 9:16 đang BẬT — đo thực tế mất 50% số video đã tải
`cfg.ALLOW_PORTRAIT` rỗng ⇒ `skip_portrait=True` cho X. Video bị loại **sau
khi đã tải xong** (tốn băng thông rồi mới xóa).

Đo thật trên 6 URL đầu từ tab Media (`cctv robbery`):

| Kích thước | Tỉ lệ | Số phận |
|---|---|---|
| 1304x734 | 1.78 | giữ |
| 720x900 | 0.80 | giữ |
| 480x546 | 0.88 | giữ |
| 720x1280 | **0.5625** | **loại (đúng 9:16)** |
| 1080x1920 | **0.5625** | **loại** |
| 720x1280 | **0.5625** | **loại** |

⇒ **3/6 = 50%** lượt tải là công cốc. X đầy clip dọc repost từ TikTok/Reels.
Muốn giữ:
```bash
export CRAWL_ALLOW_PORTRAIT="x"        # hoặc "x|tiktok|instagram"
```
Cân nhắc: clip CCTV gốc gần như luôn ngang; clip dọc thường là bản repost đã
bị crop, giữ lại sẽ làm bẩn dataset. Mặc định lọc là có chủ ý — nhưng nếu
muốn tiết kiệm băng thông thì chỗ đáng sửa là lọc theo `width/height` trong
metadata yt-dlp TRƯỚC khi tải, chứ không phải tắt bộ lọc.

### 1.9 — Quy mô 1 run: 33 lượt search, không có account nào
```
11 nhãn × 3 keyword = 33 lượt search
+ len(X_ACCOUNTS) = 0 lượt timeline   (CRAWL_X_ACCOUNTS mặc định rỗng)
```
Ít hơn Reddit (198) rất nhiều nên `execution_timeout` mặc định 3h là đủ rộng.
Lần chạy thật 2026-07-29 chỉ mất ~4 phút cho 19 lượt — nhưng đó là vì mỗi
lượt trả 0 URL nên không có gì để tải; khi phiên sống lại, thời gian sẽ tăng
mạnh vì có video thật để VLM + download.

Thêm account để quét timeline (không gán nhãn được, video vào thư mục mặc định):
```bash
export CRAWL_X_ACCOUNTS="CCTVIdiots|Crime_Watch"
```

### 1.10 — Canonical ID và giới hạn thời lượng
L1 nhận `/i/status/<id>` và `/status/<id>` → cùng 1 tweet qua 2 dạng URL
không bị tính 2 lần. Host nhận diện: `x.com`, `twitter.com`,
`mobile.twitter.com`, `mobile.x.com` — link `twitter.com` cũ vẫn dedup đúng
với `x.com`.

`max_duration_for('x')` = **1800s (30 phút)**, dùng mặc định chung, không
override riêng như Dailymotion (900s).

---

## 2. Checklist khi X ra 0 URL / `downloaded=0`

Theo đúng thứ tự, mỗi bước rẻ hơn bước sau:

1. **Phiên còn sống không?** → lệnh `verify_credentials.json` ở mục 1.1.
   401 code 32 ⇒ dừng lại, đăng nhập lại, đừng điều tra tiếp.
2. **DAG có chạy không?** X đang PAUSE:
   ```bash
   airflow dags list-runs social_crawler_x -o plain
   ```
3. **Log có `→ 0 URL` hàng loạt không?**
   ```bash
   L="logs/dag_id=social_crawler_x/run_id=<run_id>/task_id=crawl_x/attempt=1.log"
   python - "$L" <<'PY'
   import json, sys
   from collections import Counter
   c = Counter()
   for line in open(sys.argv[1]):
       try: e = str(json.loads(line).get('event',''))
       except Exception: continue
       if '→ 0 URL' in e: c['discovery ra 0 URL'] += 1
       elif '[Discovery]' in e and 'URL' in e: c['discovery CÓ URL'] += 1
       for k in ('Chưa chạy được', 'không tìm được URL nào',
                 'SKIP - 9:16', 'SKIP - no image', '[FAILED]', 'downloaded'):
           if k in e: c[k] += 1
   for k, v in c.most_common(): print(f'{v:5}  {k}')
   PY
   ```
   Toàn bộ là `discovery ra 0 URL` ⇒ có 2 khả năng, phân biệt bằng mục 1.3 vs
   1.7c: **phiên chết** (mọi lượt đều 0, browser bị đá sang `onboarding`) hay
   **bị throttle** (vài lượt đầu có kết quả rồi tụt về 0, browser vẫn đăng
   nhập bình thường). KHÔNG phải X hết nội dung.
4. **Task SUCCESS nhưng DB rỗng** → đúng như mô tả ở mục 1.2, đây là hành vi
   đã biết chứ không phải bug mới.
5. **Chỉ khi phiên đã 200 mà vẫn 0 URL** → lúc đó mới đáng nghi selector/tab:
   chạy A/B ở mục 1.7, và mở page thật in `body` ra xem X trả gì.

---

## 3. Truy vấn DB nhanh (bảng dùng chung, lọc theo `x`)

DB: `data/db/tracker.db` (đường dẫn cũ, dùng chung mọi platform).

```bash
python -c "
import sqlite3
c = sqlite3.connect('data/db/tracker.db')
print('sources     :', c.execute('SELECT COUNT(*) FROM sources WHERE source_type=\"x\"').fetchone()[0])
print('video_urls  :', list(c.execute('SELECT status, COUNT(*) FROM video_urls WHERE source_platform=\"x\" GROUP BY status')) or 'KHÔNG CÓ DÒNG NÀO')
print('fingerprints:', c.execute('SELECT COUNT(*) FROM video_fingerprints WHERE source_platform=\"x\"').fetchone()[0])
"
```
Tính tới 2026-07-31 cả 3 đều bằng 0 — X chưa có dữ liệu nào, nên **không cần
dọn gì** trước khi bật DAG (khác Reddit, nơi có 9 âm tính giả phải xóa).

---

## 4. Vận hành Airflow — LƯU Ý VỀ AIRFLOW_HOME

```bash
export AIRFLOW_HOME="$(pwd)"   # chạy từ thư mục gốc dự án — KHÔNG có hậu tố /airflow
```
Máy này có 3 giá trị AIRFLOW_HOME dễ nhầm. Nếu scheduler đang chạy, xác minh
bằng process thật trước khi tin CLI:
```bash
ps aux | grep -E 'airflow (scheduler|dag-processor)' | grep -v grep
tr '\0' '\n' < /proc/<PID>/environ | grep AIRFLOW_HOME
```

```bash
airflow dags unpause social_crawler_x        # đang PAUSE
airflow dags list-runs social_crawler_x -o plain     # dag_id POSITIONAL, không phải --dag-id
airflow tasks clear social_crawler_x -t crawl_x -s <date> -e <date> -d -y
airflow dags trigger social_crawler_x --conf '{"max_per_label": 5, "time_budget_minutes": 10}'
```

Log task: `logs/dag_id=social_crawler_x/run_id=<run_id>/task_id=crawl_x/attempt=N.log`
(JSON mỗi dòng 1 record — parse bằng `json.loads` rồi đọc field `event`).

---

## 5. Pool GPU dùng chung

Task chạy trong pool `social_crawler_gpu` (1 slot), chia sẻ với 6 DAG platform
khác để tránh 2 platform cùng nạp Qwen2.5-VL gây OOM. X hiện chạy rất nhanh
(4 phút) vì không tìm được gì — khi phiên sống lại nó sẽ giữ slot lâu hơn
nhiều. Kiểm tra: `airflow pools list`.

---

## 6. Nếu định sửa code — xếp theo giá trị/công sức

1. **Cho `unavailable_reason()` kiểm tra PHIÊN, không chỉ file** (mục 1.2) —
   giá trị cao nhất còn lại: hiện DAG báo SUCCESS trong khi không làm được gì.
   Lưu ý: KHÔNG dùng API v1.1 để kiểm tra (mục 1.1 — cho âm tính giả). Cách
   khả thi: lượt discovery ĐẦU TIÊN nếu thấy URL cuối chứa `onboarding`/
   `mode=login` thì dừng cả task và log lý do, thay vì chạy tiếp 32 lượt vô ích.
2. **Lọc 9:16 bằng metadata TRƯỚC khi tải** (mục 1.8) — đang mất 50% băng
   thông tải rồi xóa. yt-dlp đã trả `width`/`height` ở bước đọc metadata.
3. **Thêm suffix `filter:videos` cả khi dùng `CRAWL_X_QUERIES`** (mục 1.6) —
   sửa nhỏ, tránh bẫy mất filter khi override.
4. **Cân nhắc quét thêm tab Latest** (mục 1.7) — Latest có riêng 24 tweet mà
   Media không có. Đổi lại: gấp đôi số lượt discovery. Chỉ làm nếu thấy thiếu
   dữ liệu, vì Media đã chạm trần `limit` sẵn.

## 7. Trước khi kết luận "đây là bug mới"

1. **Luôn bắt đầu bằng kiểm tra phiên** (mục 1.1). Với X, ~mọi triệu chứng
   đều quy về đây. Hạn cookie đẹp KHÔNG chứng minh gì.
2. Đọc log task thật, đếm số dòng `→ 0 URL`, đừng đoán từ mô tả.
3. Query DB trực tiếp — task SUCCESS không có nghĩa là có dữ liệu (mục 1.2).
4. Tái hiện bằng script Python độc lập, KHÔNG qua Airflow.
5. Nghi Playwright → mở page thật, in `p.url`, `p.title()`, `inner_text('body')[:600]`
   ra xem CHÍNH XÁC trang trả về gì. Chính cách này đã tìm ra redirect
   `onboarding/web?mode=login` trong vài giây, sau khi log Airflow chỉ nói
   được vỏn vẹn "0 URL".
6. Đừng mang kết luận của platform khác sang X mà không đo lại: mẹo browser
   thật của Dailymotion vô tác dụng ở đây (mục 1.4), còn mẹo tab Media của
   Reddit thì chưa biết (mục 1.7).
