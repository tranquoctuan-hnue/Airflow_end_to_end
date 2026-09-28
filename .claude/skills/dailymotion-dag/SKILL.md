---
name: dailymotion-dag
description: Debug, vận hành và bảo trì DAG social_crawler_dailymotion — cookie đăng nhập hết hạn, phân trang search, token cache yt-dlp, dedup 4 tầng. Dùng khi DAG này lỗi, downloaded=0 bất thường, hoặc cần kiểm tra sức khỏe hệ thống.
---

# social_crawler_dailymotion — Runbook

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

DAG crawl video CCTV từ Dailymotion, đi qua cổng dedup 4 tầng dùng chung
(`crawler_core.pipeline.VideoPipeline`). File chính:

| File | Vai trò |
|---|---|
| `dags/social_crawler_dailymotion_dag.py` | Định nghĩa DAG, lịch chạy, execution_timeout |
| `dags/social_crawler_common.py` | `task_crawl_platform()` — logic chạy chung mọi platform |
| `platform_crawlers/scrapers.py` → `DailymotionScraper` | Sinh URL ứng viên theo nhãn/từ khóa |
| `platform_crawlers/discovery.py` → `dailymotion_api_discover()` | **Discovery ĐƯỜNG CHÍNH** — API công khai, 100 URL/request, không cần cookie (mục 1.3a) |
| `platform_crawlers/discovery.py` → `dailymotion_paginated_discover()` | Fallback quét HTML — trần cứng 20 URL/từ khóa, chỉ dùng khi API trả 0 |
| `platform_crawlers/discovery.py` → `refresh_dailymotion_login()` | Tự làm mới `access_token` bằng `refresh_token`, gọi 1 lần/lượt `discover()` |
| `crawler_core/qwen_classifier.py` → `_get_oembed_thumbnail()` | Lấy thumbnail qua oEmbed (tránh 401) |
| `crawler_core/downloader.py` → `_ensure_dailymotion_token_cache()` | Cache token tải video (tránh 401) |
| `platform_crawlers/config.py` → `max_duration_for(platform)` | Giới hạn thời lượng video theo TỪNG platform — Dailymotion 900s (15p), mặc định 1800s |
| `cookies/dailymotion_cookies.json` / `.txt` | Cookie **đăng nhập** — bắt buộc để search ra kết quả thật |
| `cookies/dailymotion_token_cache.txt` | Cache token yt-dlp — **khác** với cookie đăng nhập ở trên, đừng nhầm |

---

## 1. Quirks đặc thù của Dailymotion (đã kiểm chứng thực tế)

Đây là phần quan trọng nhất — mọi thứ dưới đây tốn nhiều giờ điều tra để tìm ra,
đọc trước khi đoán mò khi có lỗi mới.

### 1.1 — Search ẩn danh trả về SAI kết quả, không phải lỗi
Dailymotion **không báo lỗi** khi search ẩn danh không match — nó âm thầm hiện
trending fallback (phim, bóng đá, show TV không liên quan) trông như kết quả
hợp lệ. Dấu hiệu nhận biết: mở URL search bằng Playwright, tìm text
`"We couldn't find anything for"` — nếu có, nghĩa là 0 kết quả thật.

**Bắt buộc phải có cookie đăng nhập** để thấy kết quả search thật. Không có
cookie → mọi video tìm được đều là nội dung rác, VLM sẽ (đúng đắn) loại gần
hết, thỉnh thoảng có false-positive từ ảnh bìa show TV giống bố cục CCTV.

Từ 2026-07-30, cookie đăng nhập được **tự động làm mới** mỗi lượt chạy task
(xem mục 1.2) — nếu vẫn thấy dấu hiệu "no results"/nội dung rác dù cơ chế
refresh đang chạy, nghi ngờ đầu tiên là `refresh_token` đã hết hạn hẳn
(~3 tháng) chứ không phải `access_token` (đã tự lo).

### 1.2 — Cookie đăng nhập hết hạn nhanh (~10-24h) — ĐÃ TỰ ĐỘNG HÓA
`access_token` sống ngắn (~10-24h). **Đã fix**: `refresh_dailymotion_login()`
trong `discovery.py` tự làm mới `access_token` bằng `refresh_token`
(sống ~3 tháng) TRƯỚC MỖI LƯỢT `DailymotionScraper.discover()` — gọi 1 lần/
lượt task, không phải mỗi từ khóa. Không cần đăng nhập tay hàng ngày nữa.

**QUAN TRỌNG — refresh_token XOAY VÒNG (rotate):** mỗi lần refresh thành
công, server trả về `refresh_token` MỚI và vô hiệu hóa cái cũ.
`refresh_dailymotion_login()` tự ghi đè lại `cookies/dailymotion_cookies.json`
ngay sau khi refresh — nếu code này bị sửa mà QUÊN lưu lại token mới, lần
refresh kế tiếp sẽ thất bại vĩnh viễn (dùng refresh_token đã bị thu hồi).

Endpoint dùng: `POST https://graphql.api.dailymotion.com/oauth/token` với
`grant_type=refresh_token` + `client_id`/`client_secret` CÔNG KHAI của
yt-dlp (`f1a362d288c1b98099c7` — xem `_DM_CLIENT_ID` trong `discovery.py`).
Đã kiểm chứng thực tế: token mới vẫn cho search đúng kết quả thật (không
rơi về ẩn danh).

**ĐÃ XẢY RA 2026-07-31 — `refresh_token` bị THU HỒI, không phải hết hạn.**
Cookie ghi `refresh_token` hết hạn 2026-10-29 (còn 3 tháng) nhưng server vẫn từ
chối, nên **đừng tin ngày hết hạn trong cookie**:
```
POST /oauth/token → HTTP 400
{"error":"invalid_grant","error_description":"Invalid refresh token."}
```
Cùng lúc: `access_token` đã hết hạn 01:51 cùng ngày, và file cookie có mtime
2026-07-30 15:52 — tức là **refresh chưa ghi lại file lần nào kể từ đó**.

Nghi ngờ mạnh nhất là chính cơ chế XOAY VÒNG ở trên: mỗi lần refresh thành công
sẽ thu hồi token cũ, nên **một lần ghi file bị mất/bị đè là mất login vĩnh viễn**
— không có cách khôi phục ngoài đăng nhập tay. Dấu hiệu để nhận ra sớm: mtime
của `cookies/dailymotion_cookies.json` không đổi sau một lượt chạy DAG.

**Khi nào cần đăng nhập tay lại:** `refresh_token` hết hạn (~3 tháng) HOẶC bị thu
hồi như trên — `refresh_dailymotion_login()` log rõ
`"Refresh token thất bại ... cần đăng nhập lại"`. Khi thấy log này:
```bash
# 1. Mở Chrome, đăng nhập dailymotion.com (xác nhận thấy avatar/tên tài khoản)
# 2. Nhập cookie mới:
python scripts/import_browser_cookies.py dailymotion
```
Nếu báo "chưa đăng nhập" dù vừa login — thử lại vài giây sau (Chrome có
lúc chưa flush cookie DB xuống đĩa ngay).

**Kiểm tra thủ công cookie còn login thật hay chỉ còn cookie tracking**
(dùng khi nghi ngờ cơ chế tự refresh có vấn đề):
```bash
python -c "
import json, time
ck = json.load(open('cookies/dailymotion_cookies.json'))
names = {c['name'] for c in ck}
real_login = {'uid_dm','access_token','refresh_token'} & names
print('Có login thật:', bool(real_login), real_login)
for c in sorted(ck, key=lambda x: x['expires']):
    if c['name'] in ('access_token','refresh_token'):
        print(f\"  {c['name']:15} hết hạn {time.strftime('%Y-%m-%d %H:%M', time.localtime(c['expires']))}\")"
```
`v1st`, `ts`, `damd`, `usprivacy` là cookie tracking gắn cho MỌI khách kể cả
ẩn danh — **không** chứng minh đã đăng nhập, đừng bị đánh lừa (đây là bug
từng gặp: lượt kiểm tra đầu tiên dùng nhầm `v1st` làm dấu hiệu login).

### 1.3 — Search phân trang qua `?page=N`, KHÔNG lazy-load khi cuộn
Đã kiểm chứng NHIỀU LỚP (không chỉ đoán từ 1 lần thử):
- Cuộn + đợi tổng >60s (3s, 5s, 10s, 15s, 30s cộng dồn) → `scrollHeight` và
  số URL đứng yên hoàn toàn, không nhích 1px/1 URL nào.
- Theo dõi network request trong 20s sau khi cuộn → **0 request**
  search/API/graphql nào được gửi đi. Không phải vấn đề timing/chờ chưa đủ
  lâu — trang này thực sự không có listener nào lắng nghe scroll để tải
  thêm, mỗi trang render tĩnh, đầy đủ, cố định 20 kết quả ngay từ đầu.

⚠ **`?page=N` KHÔNG còn hoạt động (đo lại 2026-08-03).** Ghi chú cũ ("đổi
`?page=2`, `?page=3` để xem thêm") đã SAI — Dailymotion đổi hành vi:

```
page=1: 20 ID  dau=['x54jscj','x6ayonw']
page=2: 20 ID  dau=['x54jscj','x6ayonw']      ← y HỆT page 1
page=3: 20 ID  dau=['x54jscj','x6ayonw']
page1==page2? True    page2==page3? True
link chứa "page": KHÔNG CÓ      button: chỉ có tab + Filters, không có phân trang
cuộn 12 lần: 20 → 20 ID
```

URL vẫn giữ tham số `?page=2` nhưng nội dung bỏ qua nó. ⇒ **trang search HTML
trần CỨNG 20 kết quả/từ khóa**, không có cách nào lấy thêm: không phân trang,
không cuộn, không nút load more.

### 1.3a — Dùng API công khai thay vì quét HTML (đường CHÍNH từ 2026-08-03)
`https://api.dailymotion.com/videos?search=<kw>&limit=100&page=N&sort=relevance`
— **không cần cookie, không cần browser**:

| | HTML scrape | API |
|---|---|---|
| URL/từ khóa | **20** (trần cứng) | **100/request**, `total=1000`/query |
| Tốc độ | 20 URL / 8.5s | **500 URL / 6.5s** |
| Cookie login | BẮT BUỘC | không cần |
| Chất lượng | lẫn trending khi cookie chết | query `cctv robbery` → "Robbery CCTV", "Robbery CCTV Footage", "Luton robbery CCTV" |

Đã kiểm chứng thêm:
- Không có header rate-limit nào; 5 request liên tiếp đều 200, ~1.2s/request.
- `shorter_than` (đơn vị **PHÚT**) lọc đúng ngay tại nguồn: `shorter_than=15`
  → video dài nhất 6.5 phút; `shorter_than=5` → dài nhất 3.0 phút. Nhờ vậy khỏi
  tốn lượt yt-dlp đọc metadata rồi mới bỏ ở `match_filter` (mục 1.7).
- URL trả về tải được thật: 3/3 URL `extract_info` OK, có format, tiêu đề đúng
  chủ đề.
- Trang cuối lặp lại vài ID của trang trước → hàm đã dedupe bằng `seen`.

`dailymotion_api_discover()` là đường chính; `dailymotion_paginated_discover()`
(quét HTML) chỉ còn là **fallback tự động** khi API trả 0 URL. Tắt API:
`CRAWL_DAILYMOTION_USE_API=false`.

Đo end-to-end qua `DailymotionScraper.discover()` thật (2 nhãn × 3 từ khóa):
**600 URL trong 5.7s** — trước đó tối đa 120, và 0 khi cookie chết.

⇒ Discovery giờ KHÔNG còn phụ thuộc cookie login. Cookie vẫn được refresh mỗi
lượt vì bước TẢI cần (mục 1.5), nhưng cookie chết không còn làm discovery ra 0.

**Filter trên trang search (`button:has-text("Filters")`) — đã thử, KHÔNG dùng:**

| Nhóm | Tùy chọn | Tham số URL |
|---|---|---|
| Upload Date | Today / Past week / Past month / Past year | `dateRange=today\|past_week\|past_month\|past_year` |
| Duration | <1min / 1-5min / 5-30min / 30min-1h / >1h | `duration=less_than_1\|mins_1_5\|mins_5_30\|mins_30_60\|more_than_1h` |
| Sort by | Most recent (mặc định) / Most viewed | `sortBy=most_viewed` |

**Duration chỉ chọn được 1 giá trị/request** — đã kiểm chứng kỹ (không chỉ
click 1 lần): dù DOM dùng `<input type="checkbox">` cho từng lựa chọn
(trông như multi-select), chọn bucket thứ 2 **THAY THẾ** bucket đầu trong
URL kết quả, không cộng dồn; không có nút "Apply"/"Done" nào để tick nhiều
ô rồi áp dụng 1 lần (panel chỉ có "Close" và "Clear all", mỗi click áp dụng
+ đóng panel ngay). Test bằng cả 2 cách (đóng/mở panel giữa mỗi lần chọn,
và giữ panel mở chọn liên tiếp) đều cho cùng kết quả.

**Quyết định:** KHÔNG dùng `?duration=` để lọc video dài ngay tại bước
search (vì chỉ chọn được 1 bucket, không khớp chính xác ngưỡng mong muốn
15 phút — bucket gần nhất "5-30 min" vẫn lẫn 15-30 phút không mong muốn).
Thay vào đó enforce CHÍNH XÁC ở tầng khác — xem mục 1.7.

### 1.3b — `wait_until='networkidle'` KHÔNG BAO GIỜ đạt trên trang search
Phát hiện 2026-07-31 khi debug "0 URL (qua 1 trang)" cho mọi từ khóa. Đo trực
tiếp cùng URL, cùng cookie, cùng browser:

| `wait_until` | thời gian | kết quả `goto()` | số link đọc được SAU đó |
|---|---|---|---|
| `networkidle` | **30.0s** | ném `TimeoutError` | 8 |
| `domcontentloaded` | **0.4s** | không lỗi | 8 |

Trang luôn còn request nền nên không bao giờ "idle". Cũ code là
`except Exception: break` ⇒ lỗi CHỜ bị hiểu thành trang chết, hủy luôn cả trang
ĐÃ TẢI XONG và đọc được → mỗi từ khóa mất đúng 30 giây để trả về 0 URL
(33 từ khóa ≈ 16 phút cháy không). Đã sửa: dùng `domcontentloaded`, và goto lỗi
thì **log rồi vẫn đọc trang**, để vòng lặp tự break khi thật sự không có link.

⇒ Thấy log `0 URL (qua 1 trang)` mà mỗi lượt tốn tròn ~30s: nghi ngay điều kiện
chờ, ĐỪNG nghi cookie trước (cookie chết cho ra 8 URL rác, KHÔNG cho ra 0).

### 1.3c — Cách phân biệt "cookie chết" với "code lỗi" trong 1 lệnh
Cookie chết và discovery lỗi cho ra triệu chứng khác nhau, đừng đoán:

| Triệu chứng | Nguyên nhân |
|---|---|
| 0 URL, mỗi lượt ~30s | điều kiện chờ / goto (mục 1.3b) |
| ~8 URL rác mỗi từ khóa, VLM loại gần hết | cookie chết (mục 1.1, 1.2) |

Kiểm chứng cookie bằng cách so CÓ cookie vs ẨN DANH — nếu ra **cùng bộ ID** thì
cookie vô tác dụng:
```bash
python - <<'EOF'
import json, re
from playwright.sync_api import sync_playwright
rx=re.compile(r'/video/([A-Za-z0-9]+)')
url='https://www.dailymotion.com/search/cctv%20robbery%20footage/videos?page=1'
ck=json.load(open('cookies/dailymotion_cookies.json')); out={}
with sync_playwright() as pw:
    b=pw.chromium.launch(headless=True,args=['--no-sandbox'])
    for name, cookies in (('CÓ cookie', ck), ('ẨN DANH', None)):
        ctx=b.new_context(); ctx.add_cookies(cookies) if cookies else None
        p=ctx.new_page(); p.goto(url,wait_until='domcontentloaded',timeout=30_000)
        p.wait_for_timeout(6000)          # ⚠ phải ≥5s, khối "no results" render CHẬM
        ids={rx.search(a.get_attribute('href') or '').group(1)
             for a in p.query_selector_all('a[href]') if rx.search(a.get_attribute('href') or '')}
        t=p.inner_text('body').lower(); out[name]=ids
        print(f"{name}: {len(ids)} ID | chưa đăng nhập={'connect' in t} | "
              f"không có kết quả={'couldn' in t}")
        ctx.close()
    b.close()
print('=> cookie VÔ TÁC DỤNG' if out['CÓ cookie']==out['ẨN DANH'] else '=> cookie CÓ tác dụng')
EOF
```
⚠ **Phải đợi ≥5s** trước khi đọc `inner_text`. Đọc ở 2.5s thì khối "We couldn't
find anything" chưa render → tưởng là có kết quả thật (đã bị lừa đúng như vậy
một lần trong phiên debug này, kết luận đầu tiên sai và phải test lại).

`dailymotion_paginated_discover()` giờ tự kiểm tra dấu hiệu này ở trang 1
(`_dm_no_real_results()`) và trả về `[]` thay vì 8 URL gợi ý — quan trọng vì URL
rác đi qua pipeline sẽ bị ghi `skipped_not_cctv`, là **trạng thái CUỐI**, nên L0
chặn chúng vĩnh viễn dù sau này cookie đã sống lại.

### 1.4 — `window.scrollBy()` không hoạt động trên trang này
Nếu viết thêm code Playwright cho Dailymotion, dùng `page.mouse.wheel(0, N)`
hoặc `page.evaluate('window.scrollTo(0, document.body.scrollHeight)')` —
KHÔNG dùng `scrollBy()`, đã kiểm chứng nó không di chuyển trang
(`scrollY` luôn = 0 dù gọi nhiều lần).

### 1.5 — yt-dlp Dailymotion extractor dùng `client_id` dùng chung toàn cầu
`f1a362d288c1b98099c7` hardcode trong yt-dlp, bị rate-limit ngẫu nhiên (401)
vì MỌI người dùng yt-dlp trên thế giới share chung. Ảnh hưởng 2 chỗ khác nhau:
- **Lấy thumbnail** (`qwen_classifier.py`) → đã fix bằng oEmbed công khai
  (`dailymotion.com/services/oembed`), không đụng client_id này.
- **Tải video thật** (`crawler_core/downloader.py`) → yt-dlp tự cache
  `client_token` vào cookie nếu có sẵn cookiefile — đã fix bằng
  `_ensure_dailymotion_token_cache()` (file riêng, KHÔNG phải cookie đăng
  nhập ở mục 1.2). Nếu thấy lại lỗi 401 lúc TẢI (không phải lúc search),
  kiểm tra file `cookies/dailymotion_token_cache.txt` có tồn tại và có dòng
  `client_token` chưa.

### 1.6 — Video nằm trong iframe, không phải `<video>` ở trang chính
Nếu cần chụp frame trực tiếp (Playwright) thay vì thumbnail: video thật nằm
trong iframe `geo.dailymotion.com/player/...`, phải lấy qua
`page.frames` rồi tìm frame có `'dailymotion.com/player' in f.url`, KHÔNG
`page.query_selector('video')` ở trang chính (luôn ra 0 kết quả).

### 1.7 — Giới hạn thời lượng video: enforce cục bộ, KHÔNG lọc bằng URL search
Dailymotion hay lẫn phim/show TV dài (đã gặp thực tế: tập phim 32 phút) vào
kết quả search CCTV. Vì filter `?duration=` của Dailymotion chỉ chọn được
1 bucket (mục 1.3) và không có bucket khớp đúng ngưỡng mong muốn, giới hạn
thời lượng được enforce ở **tầng downloader**, không phải tầng search:

```python
# platform_crawlers/config.py
PLATFORM_MAX_DURATION_S = {'dailymotion': 900}   # 15 phút, override CRAWL_DAILYMOTION_MAX_DURATION
DEFAULT_MAX_DURATION_S  = 1800                    # 30 phút, mọi platform khác

# dags/social_crawler_common.py — GenericDownloader nhận đúng giới hạn riêng platform
downloader = GenericDownloader(platform=platform, max_duration=cfg.max_duration_for(platform))
```

Video >15 phút bị `match_filter` (trong `crawler_core/downloader.py`) chặn
**TRƯỚC KHI TẢI THẬT** (yt-dlp đọc metadata rồi bỏ qua, không tốn băng
thông tải cả file) — không phải bug nếu thấy log
`dài {N}s > giới hạn 900s — bỏ qua` cho video 16+ phút, đây là hành vi ĐÚNG
thiết kế. Đổi ngưỡng: sửa `CRAWL_DAILYMOTION_MAX_DURATION` (giây) trong env,
hoặc sửa trực tiếp `PLATFORM_MAX_DURATION_S['dailymotion']` trong config.py.

Kiểm tra nhanh ngưỡng đang áp dụng đúng chưa:
```bash
python -c "
import sys; sys.path.insert(0,'.')
from crawler_core.downloader import GenericDownloader
from platform_crawlers import config as cfg
dl = GenericDownloader(platform='dailymotion', max_duration=cfg.max_duration_for('dailymotion'))
filt = dl._build_match_filter()
for mins in [10, 15, 16, 32]:
    r = filt({'duration': mins*60})
    print(f'{mins} phút -> {\"BỊ CHẶN\" if r else \"QUA\"}')"
```

### 1.8 — `href` trên trang search là đường dẫn TƯƠNG ĐỐI
Thẻ `<a>` của Dailymotion trả `/video/x8ewysm`, KHÔNG phải URL đầy đủ. Nếu
scraper yield thẳng href, lỗi không lộ ra ở chỗ discovery mà nổ ở 3 chỗ tận
cùng, nhìn tưởng 3 bug khác nhau:

```
WARNING - [SKIP - no image] /video/x8ewysm
ERROR   - ERROR: [generic] '/video/x4cdgfd' is not a valid URL
WARNING - og:image fallback thất bại: No scheme supplied. Perhaps you meant https:///video/...
WARNING - Playwright frame capture thất bại: Cannot navigate to invalid URL
```

Thấy `https:///` (3 dấu gạch) hoặc `is not a valid URL` ⇒ luôn là bug URL
tương đối, không phải lỗi mạng/cookie. `dailymotion_paginated_discover()`
dựng lại URL canonical từ ID chứ không dùng href thô — vừa hết lỗi này vừa
tránh dedup tính `/video/xxx`, `/fr/video/xxx_slug`, `...?playlist=` là 3
video khác nhau:

```python
m = re.compile(r'/video/[A-Za-z0-9]+').search(href)
this_page_ids.add(f'https://www.dailymotion.com{m.group(0)}')
```

`VideoPipeline._process()` giờ có chốt chặn URL không bắt đầu bằng
`http://`/`https://`: trả `ERROR` kèm log nêu rõ `source_page` (biết ngay
scraper nào sai) và KHÔNG ghi URL rác vào DB. Áp dụng cho mọi platform, nên
bug cùng loại ở scraper khác cũng bị bắt tại cổng.

Di chứng của bug này trong DB: URL tương đối đã bị ghi `skipped_not_cctv`
(âm tính giả — bị loại vì không lấy được ảnh, không phải vì không phải
CCTV). Chúng KHÔNG chặn lần chạy sau vì L0 so khớp chuỗi URL chính xác, mà
URL đúng (`https://...`) là chuỗi khác — chỉ làm lệch thống kê. Dọn:

```sql
-- Đếm trước
SELECT COUNT(*) FROM video_urls WHERE url LIKE '/video/%';
-- Xóa (backup DB trước, xem mục 3)
DELETE FROM video_urls WHERE url LIKE '/video/%';
```
Lưu ý đừng dùng điều kiện rộng `url LIKE '/%'` — `video_fingerprints.url`
chứa đường dẫn file local hợp lệ (`/home/.../data/videos/...`) cũng khớp.

---

## 2. Checklist khi thấy `downloaded=0` hoặc kết quả bất thường

1. **Cookie đăng nhập còn sống không?** → chạy lệnh ở mục 1.2
2. **Search có thật sự ra kết quả không?** (test độc lập, không qua Airflow):
   ```bash
   python -c "
   import sys; sys.path.insert(0,'.')
   from platform_crawlers.discovery import dailymotion_paginated_discover
   urls = dailymotion_paginated_discover('cctv robbery footage', limit=20,
       cookies_json='cookies/dailymotion_cookies.json')
   print(len(urls), urls[:5])"
   ```
3. **Nếu URL ra rồi nhưng downloaded vẫn 0** → xem log tìm dòng
   `[SKIP]`/`not_cctv` (VLM loại) hay `[FAILED]`/`Không tải được` (download
   lỗi — kiểm tra `match_filter` do video quá dài, Dailymotion giới hạn
   `max_duration=900s` = 15 phút, KHÁC các platform khác (1800s) — xem
   mục 1.7, đây là hành vi ĐÚNG không phải bug) hay `dup_url` (đã xử lý từ
   trước, xem mục 3 nếu muốn buộc xử lý lại).
4. **Log có `is not a valid URL`, `No scheme supplied`, `https:///`, hoặc
   `Cannot navigate to invalid URL`?** → bug URL tương đối, xem mục 1.8. Dấu
   hiệu nhận biết: URL trong log bắt đầu bằng `/video/` chứ không phải `https://`.
5. **Xác minh ảnh dùng để phân loại có đúng nội dung video không** — tải
   thumbnail qua oEmbed và xem trực tiếp trước khi nghi ngờ VLM sai:
   ```bash
   python -c "
   import requests
   r = requests.get('https://www.dailymotion.com/services/oembed?url=https://www.dailymotion.com/video/<ID>&format=json')
   print(r.json()['thumbnail_url'])"
   ```

---

## 3. Truy vấn DB nhanh (bảng dùng chung, lọc theo `dailymotion`)

DB: `data/db/tracker.db` (đường dẫn cũ, dùng chung mọi platform).

```bash
python -c "
import sqlite3
c = sqlite3.connect('data/db/tracker.db')
print('sources (từ khóa đã search):',
      c.execute('SELECT COUNT(*) FROM sources WHERE source_type=\"dailymotion\"').fetchone()[0])
print('video_urls theo status:')
for r in c.execute('SELECT status, COUNT(*) FROM video_urls WHERE source_platform=\"dailymotion\" GROUP BY status'):
    print(' ', r)
print('fingerprints (video unique đã tải):',
      c.execute('SELECT COUNT(*) FROM video_fingerprints WHERE source_platform=\"dailymotion\"').fetchone()[0])
"
```

**Reset dedup L0** cho Dailymotion (để VLM xử lý lại URL cũ — tốn GPU, chỉ
làm khi thật sự cần kiểm chứng lại, KHÔNG nên làm mỗi lần chạy):
```bash
# LUÔN backup trước khi xóa
python -c "
import sqlite3, json
c = sqlite3.connect('data/db/tracker.db')
rows = c.execute('SELECT * FROM video_urls WHERE source_platform=\"dailymotion\"').fetchall()
cols = [d[0] for d in c.execute('SELECT * FROM video_urls LIMIT 0').description]
json.dump([dict(zip(cols,r)) for r in rows], open('/tmp/dm_video_urls_backup.json','w'), indent=2, ensure_ascii=False)
print(len(rows), 'dòng đã backup')
"
# Rồi mới xóa
python -c "
import sqlite3
c = sqlite3.connect('data/db/tracker.db')
print(c.execute('DELETE FROM video_urls WHERE source_platform=\"dailymotion\"').rowcount, 'dòng đã xóa')
c.commit()
"
```
Lưu ý: xóa `video_urls` KHÔNG xóa `video_fingerprints` — video đã tải vẫn
được bảo vệ khỏi tải trùng (sẽ bị L2/L3/L4 chặn lại sau khi tải lại và
tốn băng thông, xem chi tiết trong hội thoại gốc nếu cần).

---

## 4. Vận hành Airflow — LƯU Ý QUAN TRỌNG VỀ AIRFLOW_HOME

Máy này có **3 giá trị AIRFLOW_HOME khác nhau** dễ gây nhầm lẫn. Scheduler
thật đang chạy dùng:
```bash
export AIRFLOW_HOME="$(pwd)"   # chạy từ thư mục gốc dự án — ĐÚNG — không có hậu tố /airflow
```
Xác minh bằng cách so khớp với process thật đang chạy trước khi tin lệnh
CLI trả về gì:
```bash
ps aux | grep -E 'airflow (scheduler|dag-processor)' | grep -v grep
tr '\0' '\n' < /proc/<PID>/environ | grep AIRFLOW_HOME
```

Lệnh thường dùng:
```bash
airflow dags list-runs social_crawler_dailymotion -o plain   # dag_id là POSITIONAL, không phải --dag-id
airflow tasks clear social_crawler_dailymotion -t crawl_dailymotion -s <date> -e <date> -d -y
airflow dags trigger social_crawler_dailymotion --conf '{"max_per_label": 5, "time_budget_minutes": 10}'
```

Log task nằm ở:
```
logs/dag_id=social_crawler_dailymotion/run_id=<run_id>/task_id=crawl_dailymotion/attempt=N.log
```

---

## 5. Pool GPU dùng chung

Task này chạy trong pool `social_crawler_gpu` (1 slot) — chia sẻ với 6 DAG
platform khác (`social_crawler_youtube/tiktok/instagram/x/reddit/vimeo`) để
tránh 2 platform cùng nạp Qwen2.5-VL lên GPU gây OOM. Nếu thấy task đứng ở
trạng thái "queued" lâu bất thường, kiểm tra pool có đang bị platform khác
giữ slot không:
```bash
airflow pools list
```

---

## 6. Trước khi kết luận "đây là bug mới"

Thứ tự điều tra đã chứng minh hiệu quả trong session trước — làm theo đúng
thứ tự để không tốn công đoán mò:
1. Đọc log task thật (không đoán từ mô tả người dùng)
2. Query DB trực tiếp xem trạng thái thật (đừng tin log tóm tắt "lũy kế")
3. Tái hiện lỗi bằng script Python độc lập, KHÔNG qua Airflow (nhanh hơn
   nhiều lần lặp debug qua trigger DAG)
4. Nếu nghi ngờ Playwright/discovery: mở page thật, in ra text/DOM để xem
   CHÍNH XÁC những gì trang trả về, đừng giả định
5. So sánh với trải nghiệm trình duyệt thật của user nếu có thể — đã từng
   phát hiện bug lớn (thiếu cookie đăng nhập) chỉ nhờ user báo "tôi thấy
   khác khi tự bấm link"
6. Khi user nghi ngờ 1 kết luận trước đó ("có cần đợi lâu hơn không?",
   "chắc chưa hết cách chọn đâu?") — ĐỪNG bảo vệ kết luận cũ, kiểm chứng lại
   NGHIÊM NGẶT HƠN: tăng thời gian chờ + theo dõi network request (không chỉ
   đo DOM), hoặc thử tương tác UI theo nhiều cách khác nhau (đóng/mở panel
   giữa mỗi lần chọn VÀ giữ panel mở chọn liên tiếp) trước khi khẳng định
   lại. 2 lần trong session này việc user hỏi lại đã dẫn đến kiểm chứng chặt
   hơn (dù kết luận cuối cùng vẫn giữ nguyên) — nghi ngờ của user luôn đáng
   để test lại bằng bằng chứng mới, không phải chỉ nhắc lại lý do cũ.
