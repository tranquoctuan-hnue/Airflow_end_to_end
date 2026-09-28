---
name: reddit-dag
description: Debug, vận hành và bảo trì DAG social_crawler_reddit — .json chặn 403 khi ẩn danh nhưng trả 200 khi có cookie đăng nhập, 3 chế độ auth (user OAuth / app-only / Playwright fallback 198 lượt browser), 429 khi lấy thumbnail, âm tính giả skipped_not_cctv bị L0 chặn vĩnh viễn. Dùng khi DAG này lỗi, downloaded=0, task chạy quá lâu, hoặc cần bật/nhập lại OAuth và cookie Reddit.
---

# social_crawler_reddit — Runbook

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

DAG crawl video CCTV từ Reddit, đi qua cổng dedup 4 tầng dùng chung
(`crawler_core.pipeline.VideoPipeline`). File chính:

| File | Vai trò |
|---|---|
| `dags/social_crawler_reddit_dag.py` | Định nghĩa DAG — schedule `ROTATION_SCHEDULE` (10 phút, xoay vòng liên tục qua pool 1 slot — xem mục "Chạy TUẦN TỰ LIÊN TỤC" trong `social_crawler_common.py`), `execution_timeout=4h` (DÀI hơn 3h mặc định, lý do ở mục 1.3) |
| `dags/social_crawler_common.py` | `task_crawl_platform()` — logic chạy chung mọi platform, chứa trần `max_per_label`/`max_per_platform`/time budget |
| `platform_crawlers/scrapers.py` → `RedditScraper` | Sinh cặp (keyword × nguồn): 1 search toàn Reddit + 5 subreddit cho MỖI keyword |
| `platform_crawlers/discovery.py` → `reddit_discover()` | Discovery core — CÓ query thì luôn đi tab Media; không query mới dùng API listing |
| `platform_crawlers/discovery.py` → `_reddit_discover_media_tab()` | **Đường chính**: UI tab Media + `sort=relevance&t=all` + cuộn, rồi lọc video |
| `platform_crawlers/discovery.py` → `_reddit_keep_videos()` | Lọc bài thật sự có video qua `/api/info.json` (100 ID/request, cần cookie) |
| `platform_crawlers/discovery.py` → `_reddit_oauth_token()` | Lấy token OAuth, cache trong process; 2 chế độ user-login / app-only |
| `platform_crawlers/discovery.py` → `_reddit_discover_playwright()` | Fallback cũ, giờ chỉ còn dùng cho listing `r/<sub>/new` khi KHÔNG có OAuth |
| `platform_crawlers/config.py` | `REDDIT_SUBREDDITS`, `REDDIT_GLOBAL_SEARCH`, `REDDIT_HAS_OAUTH`, `REDDIT_HAS_USER_LOGIN` |
| `crawler_core/dedup.py` → `_PLATFORM_RULES['reddit']` | Canonical ID (L1): `/comments/<id>`, `v.redd.it/<id>`, `redd.it/<id>` (short link) |
| `crawler_core/downloader.py` → `_COOKIE_PATHS['reddit']` | `cookies/reddit_cookies.txt` cho yt-dlp lúc TẢI |
| `cookies/reddit_cookies.json` / `.txt` | Cookie đăng nhập — dùng cho tab Media (Playwright), lọc video (`info.json`), và lúc tải (yt-dlp). KHÔNG liên quan OAuth |
| `scripts/save_platform_cookies.py reddit` | Lưu cookie bằng cách đăng nhập tay trong browser mà script mở ra |
| `scripts/import_browser_cookies.py reddit` | Lấy cookie từ Chrome thường đang đăng nhập sẵn |

---

## 0. Trạng thái đã kiểm chứng (2026-07-31) — đọc trước khi debug

Bốn dữ kiện dưới đây quyết định gần như mọi triệu chứng bạn sẽ gặp:

| Hạng mục | Trạng thái thực tế | Hệ quả |
|---|---|---|
| DAG `social_crawler_reddit` | **đang PAUSE** (`is_paused=1` trong `airflow.db`; chỉ `social_crawler_dailymotion` đang bật) | Chưa từng có run nào — `logs/dag_id=social_crawler_reddit/` KHÔNG tồn tại |
| OAuth | **CHƯA cấu hình** — `.env` không có `REDDIT_CLIENT_ID/SECRET` (`cfg.REDDIT_HAS_OAUTH = False`) | Không ảnh hưởng discovery: mọi query đều đi tab Media bất kể có OAuth hay không (mục 1.11). OAuth chỉ còn dùng cho listing `r/<sub>/new` |
| Cookie đăng nhập | **CÒN HẠN** (nhập lại 2026-07-31 09:2x từ Chrome Default) — user `Zenda-abama`, `over_18=True`. `reddit_session` hạn 2027-01-28, `token_v2` hạn 2026-08-01 | **Bắt buộc** cho cả 3 việc: tab Media thấy được NSFW, `info.json` lọc video, yt-dlp tải. Mất cookie là hỏng cả chuỗi |
| Kết quả trong DB | 10 URL reddit: 9 `skipped_not_cctv` + 1 `pending`; **0 fingerprint** | Chưa từng tải được video Reddit nào. 9 URL kia là ÂM TÍNH GIẢ — xem mục 1.5 |

⚠ **BẪY: `import_browser_cookies.py --list` báo `reddit ✓ đã login` KỂ CẢ KHI
COOKIE ĐÃ HẾT HẠN.** Nó chỉ so TÊN cookie (`hits = [k for k in keys if k in names]`),
không hề kiểm tra `expires`. Đã bị lừa 1 lần: `--list` báo `✓ đã login (token_v2)`
trong khi `token_v2` hết hạn từ 24 ngày trước. Luôn kiểm tra hạn thật:
```bash
python -c "
import sys, time, datetime; sys.path.insert(0,'.'); sys.path.insert(0,'scripts')
from import_browser_cookies import load_jar, cookies_for, normalize_expires
now = time.time()
for prof in [None, 'Profile 1']:
    try: jar = load_jar('chrome', prof)
    except Exception: continue
    print(f\"--- chrome [{prof or 'Default'}] ---\")
    for c in sorted(cookies_for(jar, ['.reddit.com']), key=lambda x: x.name):
        e = normalize_expires(c.expires)
        when = datetime.datetime.fromtimestamp(e).strftime('%Y-%m-%d %H:%M') if e > 0 else 'session'
        print(f\"  {c.name:32} {when:17} {'CÒN HẠN' if (e < 0 or e > now) else '*** HẾT HẠN ***'}\")"
```
Cookie đăng nhập Reddit THẬT là `reddit_session` + `token_v2` còn hạn. Có
`loid`/`edgebucket`/`csv`/`ads_cookie` mà thiếu 2 cái đó = **chưa đăng nhập**
(giống chuyện `v1st` của Dailymotion — cookie tracking gắn cho mọi khách).
Thấy cookie `g_state` ⇒ tài khoản đăng nhập **qua Google** ⇒ KHÔNG dùng được
`save_platform_cookies.py` (Playwright bị Google chặn), phải đi đường
`import_browser_cookies.py` từ Chrome thường.

**Quy trình nhập lại cookie khi hết hạn** (đã chạy thành công 2026-07-31):
```bash
# 1. Bạn (người dùng) tự đăng nhập reddit.com trong Chrome profile Default —
#    bằng tay, KHÔNG qua Playwright. Bật luôn "Show mature content (18+)"
#    tại https://www.reddit.com/settings/account nếu chưa bật.
# 2. Nhập vào crawler (ghi cả .json cho Playwright và .txt cho yt-dlp):
python scripts/import_browser_cookies.py reddit
# 3. XÁC MINH THẬT — đừng tin dấu ✓ của script (xem bẫy ở trên).
#    ⚠ PHẢI dùng UA Chrome ĐẦY ĐỦ — 'Mozilla/5.0' ngắn bị 403, xem mục 1.1.
python -c "
import requests
UA = ('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36')
ck = {}
for line in open('cookies/reddit_cookies.txt'):
    p = line.rstrip('\n').split('\t')
    if len(p) >= 7 and not line.startswith('#'): ck[p[5]] = p[6]
r = requests.get('https://www.reddit.com/api/me.json',
                 headers={'User-Agent': UA}, cookies=ck, timeout=20)
print('HTTP', r.status_code, '| ct =', r.headers.get('content-type','')[:16])
if r.status_code == 200:
    d = r.json().get('data', {})
    print('user =', d.get('name'), '| over_18 =', d.get('over_18'))"
# Đúng thì in: HTTP 200 | ct = application/json  /  user = <nick> | over_18 = True
```
`token_v2` chỉ sống ~24h còn `reddit_session` sống tới 2027-01 — **chưa kiểm
chứng** phiên có tự sống tiếp sau khi `token_v2` hết hạn hay không (Reddit
thường tự phát lại `token_v2` từ `reddit_session`, nhưng cookie đã lưu ra file
là ảnh tĩnh, không tự làm mới được như Dailymotion). Nếu sau 1-2 ngày thấy
Reddit trả 403/rỗng thì chạy lại quy trình trên trước khi nghi ngờ chỗ khác.

10 URL đó đến từ DAG cũ `social_cctv_video_crawler` (task `crawl_reddit`,
run `scheduled__2026-07-29T02:00:00+00:00`) — log của nó là bằng chứng thực
tế duy nhất hiện có về Reddit, dùng lại được:

```
logs/dag_id=social_cctv_video_crawler/run_id=scheduled__2026-07-29T02:00:00+00:00/task_id=crawl_reddit/attempt=1.log
```

**Đã sửa ngày 2026-07-31:** discovery chuyển sang tab Media + lọc video
(mục 1.11). Đo end-to-end sau khi sửa: `reddit_discover(None, 'cctv robbery', 40)`
→ 40 URL video trong 53s, tỉ lệ có video 96/120 (80%). Nhánh subreddit cũng
đáng giữ: r/PublicFreakout cho 36/40 URL KHÔNG trùng với search toàn Reddit.

**Việc nên làm tiếp, theo thứ tự:**
1. Xử lý lãng phí quota (mục 1.3) — ~2.8h/run chạy không công, hoặc né tạm
   bằng `CRAWL_KEYWORDS_PER_LABEL=1`.
2. Dọn 9 URL `skipped_not_cctv` âm tính giả (mục 3) — nếu không, L0 chặn
   chúng vĩnh viễn dù nguyên nhân gốc đã hết.
3. `airflow dags unpause social_crawler_reddit` — làm SAU 2 bước trên.

---

## 1. Quirks đặc thù của Reddit (đã kiểm chứng thực tế)

### 1.1 — `.json` chặn 403 khi ẩn danh, nhưng CÓ COOKIE ĐĂNG NHẬP thì trả 200
Kiểm chứng ngày 2026-07-31 từ máy này, cùng một URL, khác nhau đúng ở chỗ có
gửi cookie đăng nhập hay không:

| URL | Ẩn danh | Kèm cookie đăng nhập |
|---|---|---|
| `www.reddit.com/r/<sub>/new.json` | **403** (trang "Blocked" ~190KB) | **200** + JSON đầy đủ metadata |
| `www.reddit.com/r/<sub>/search.json?...&restrict_sr=1` | 403 | **200** |
| `www.reddit.com/search.json?q=...&include_over_18=on` | 403 | **200** |
| `www.reddit.com/api/me.json` | (chỉ trả `loid`) | **200** + `name`, `over_18` |
| `old.reddit.com/r/<sub>/new.json` | **403** (đổi subdomain KHÔNG lách được) | — |
| `www.reddit.com/oembed?url=...` | **403** — xem mục 1.6 | — |
| `www.reddit.com/search/?q=...` (HTML) | 200 nhưng chỉ ~8.4KB = vỏ JS rỗng | — |

⚠ **Sửa lại một kết luận sai của chính runbook này:** bản đầu ghi "403, không
có đường lách" và "fallback PHẢI dùng Playwright". Sai — cái bị chặn là truy
cập **ẩn danh**, không phải bản thân endpoint `.json`. Có cookie
`reddit_session` còn hạn thì `.json` hoạt động bình thường, và đó là đường
tốt hơn Playwright rất nhiều (xem mục 1.11).

Điều vẫn ĐÚNG: HTML thô của trang search chỉ ~8.4KB vỏ JS, nên nếu đã chọn
đường HTML thì buộc phải có browser. Nhưng không cần đi đường HTML nữa.

⚠ **Cookie đúng vẫn 403 nếu User-Agent quá ngắn.** Cùng cookie, cùng URL
`api/me.json`, chỉ khác header UA:

| User-Agent | Kết quả |
|---|---|
| `Mozilla/5.0` | **403** `text/html` trang "Blocked", không có header ratelimit |
| `Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36` | **200** `application/json` |

Bug này rất dễ chẩn đoán sai thành "cookie hết hạn" rồi đi đăng nhập lại vô
ích. Dấu hiệu phân biệt: 403 do UA thì body là **HTML** `<title>Blocked</title>`
và **không có** header `x-ratelimit-*`; cookie hết hạn thật thì `api/me.json`
vẫn trả JSON nhưng thiếu field `name`. Luôn in cả `status_code` và
`content-type` khi debug, đừng gọi `.json()` ngay (sẽ nổ `JSONDecodeError` và
che mất mã lỗi thật — đã bị đúng lỗi này 1 lần).

Rate-limit của đường `.json` + cookie (đọc từ response header, đã đo thật):
```
x-ratelimit-remaining: 79      x-ratelimit-used: 21      x-ratelimit-reset: 456
```
⇒ **100 request / cửa sổ 10 phút**. Đo thực tế: 15 request tuần tự = 14.8s
(≈1.0s/request), 15/15 đều HTTP 200, `remaining` giảm đúng 1 mỗi request.

Chi tiết về đường này và so sánh với 2 đường còn lại: **mục 1.11**.

Lệnh tự kiểm chứng lại khi nghi ngờ Reddit đổi chính sách:
```bash
python -c "
import requests
UA='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
for name,u in [
  ('json ẩn danh','https://www.reddit.com/r/CaughtOnCamera/new.json?limit=5'),
  ('oembed','https://www.reddit.com/oembed?url=https%3A%2F%2Fwww.reddit.com%2Fr%2Fx%2Fcomments%2Fabc%2Fy%2F&format=json'),
  ('search HTML','https://www.reddit.com/search/?q=cctv+robbery&sort=new')]:
    r = requests.get(u, headers={'User-Agent': UA}, timeout=20)
    print(f'{name:14} HTTP {r.status_code}  len={len(r.text)}')"
```

### 1.2 — BA chế độ auth, chênh lệch chất lượng rất lớn
`reddit_discover()` tự chọn chế độ theo env đang có. Biết đang ở chế độ nào
là điều kiện tiên quyết để hiểu log:

| Chế độ | Điều kiện | Log nhận biết | Chất lượng |
|---|---|---|---|
| **User OAuth** (tốt nhất) | `REDDIT_CLIENT_ID` + `SECRET` + `USERNAME` + `PASSWORD` | `Reddit OAuth OK — user login (<nick>) — thấy được NSFW` | Nhanh (1 HTTP request/nguồn), lọc sẵn bài có video, thấy NSFW |
| **App-only OAuth** | chỉ `CLIENT_ID` + `SECRET` | `Reddit OAuth OK — app-only — KHÔNG thấy bài NSFW` | Nhanh + lọc video, nhưng mất bài NSFW |
| **Playwright fallback** | không có gì | `chưa có REDDIT_CLIENT_ID/SECRET → fallback Playwright` | Chậm ~20-40s/nguồn, KHÔNG lọc được video, không NSFW nếu cookie hết hạn |

Cấu hình OAuth — thêm vào `.env` (KHÔNG phải cookie, hai thứ độc lập nhau):
```
REDDIT_CLIENT_ID=<id ngay dưới tên app tại reddit.com/prefs/apps>
REDDIT_CLIENT_SECRET=<dòng secret>
REDDIT_USERNAME=<nick>
REDDIT_PASSWORD=<mật khẩu>          # có 2FA: "matkhau:OTP"
```
Tạo app: https://www.reddit.com/prefs/apps → "create app" → type **script**
→ redirect uri `http://localhost:8080`.

**Vì sao user login quan trọng, không phải chuyện nhỏ:** clip CCTV/crime
trên Reddit rất hay bị gắn NSFW, và app-only auth KHÔNG thấy chúng —
`include_over_18=on` trong URL search chỉ có tác dụng khi token là user
login VÀ tài khoản đã bật **"I am over 18"** trong Settings. Thiếu 1 trong
2 điều kiện đó thì tham số này bị bỏ qua âm thầm, không báo lỗi.

**Bẫy khi debug OAuth:** Reddit trả **HTTP 200** kèm `{"error": "invalid_grant"}`
khi sai mật khẩu / thiếu OTP — không phải 401. Code đã xử lý đúng và log
`Reddit OAuth không có token: ...`; thấy log đó thì KHÔNG phải lỗi mạng, mà
là sai credentials (2FA thì phải là `REDDIT_PASSWORD="matkhau:OTP"`).

Token được cache trong biến module `_reddit_token` → **1 lần lấy token cho
cả task** (1 task Airflow = 1 process), không phải mỗi keyword. Nếu thấy log
`Reddit OAuth OK` xuất hiện nhiều lần trong 1 task thì cache đang bị hỏng.

Kiểm tra chế độ hiện tại mà không cần chạy DAG:
```bash
python -c "
import sys; sys.path.insert(0,'.')
from dotenv import load_dotenv; load_dotenv('.env')
from platform_crawlers import config as cfg
print('HAS_OAUTH      =', cfg.REDDIT_HAS_OAUTH)
print('HAS_USER_LOGIN =', cfg.REDDIT_HAS_USER_LOGIN)
from platform_crawlers.discovery import _reddit_oauth_token
print('token         =', 'LẤY ĐƯỢC' if _reddit_oauth_token() else 'KHÔNG')"
```

### 1.3 — Không có OAuth = 198 lượt mở browser, tốn 60-90+ phút
Con số 198 không phải phỏng đoán, tính ra từ config thực tế:

```
11 nhãn × 3 keyword/nhãn × (1 search toàn Reddit + 5 subreddit) = 198 lượt discover
```

Kiểm chứng lại con số này khi config thay đổi:
```bash
python -c "
import sys; sys.path.insert(0,'.')
from platform_crawlers import labels as L, keywords as K, config as cfg
n = len(L.enabled_labels()) * K.PER_LABEL * ((1 if cfg.REDDIT_GLOBAL_SEARCH else 0) + len(cfg.REDDIT_SUBREDDITS))
print(f'{len(L.enabled_labels())} nhãn × {K.PER_LABEL} kw × '
      f'{(1 if cfg.REDDIT_GLOBAL_SEARCH else 0) + len(cfg.REDDIT_SUBREDDITS)} nguồn = {n} lượt discovery')"
```

Đây là lý do `execution_timeout` của Reddit đặt 4h thay vì 3h.

⚠ **ĐỪNG hạ xuống 3h.** Đo sau khi chuyển sang tab Media (mục 1.11):
**~54 giây/lượt** ⇒ 198 lượt ≈ **3 giờ CHỈ RIÊNG discovery**, chưa tính VLM
và download. Mà giờ discovery trả về gần như toàn video thật nên phần
VLM/download cũng nặng hơn hẳn lúc trước (trước đây rác bị loại rất nhanh).

✅ **Lãng phí đã SỬA (2026-07-31).** Trước đây trần `max_per_label` (30) chỉ
được kiểm tra SAU khi generator đã yield — lượt discovery đã cuộn xong ~54s rồi
mới bị `continue` bỏ đi. Mỗi nhãn 18 lượt (3 kw × 6 nguồn) mà quota chỉ 30 URL
⇒ **17/18 lượt mỗi nhãn là công cốc (~2.8h/lần chạy)**. Đã sửa 2 chỗ:

- DAG truyền callback `label_full`/`time_up` vào scraper
  (`build_platform_dag` → `get_scraper`), scraper bỏ qua HẲN lượt discovery của
  nhãn đã đủ — xem `BaseScraper.iter_label_keywords()`. `RedditScraper` còn
  kiểm tra thêm trong vòng lặp subreddit vì 1 từ khóa sinh 6 lượt.
- `MAX_PER_LABEL` mặc định giờ là `MAX_PER_TARGET × KEYWORDS_PER_LABEL`
  (100 × 3 = 300) nên quota không còn nhỏ hơn lượng URL 1 từ khóa lấy về.
  Cân bằng nhãn giờ do con trỏ xoay vòng lo, không bóp quota nữa.

Muốn giảm khối lượng:
```bash
# Bỏ 5 subreddit, chỉ search toàn Reddit → 33 lượt thay vì 198
export CRAWL_REDDIT_SUBS=""            # ⚠ KHÔNG có tác dụng — xem cảnh báo dưới
export CRAWL_KEYWORDS_PER_LABEL=1      # 11 nhãn × 1 kw × 6 nguồn = 66 lượt
```
⚠ `CRAWL_REDDIT_SUBS=""` (chuỗi rỗng) **không** xóa được danh sách subreddit:
`_env_list()` coi chuỗi rỗng là "chưa set" và trả về default 5 subreddit.
Muốn thu hẹp thật thì đặt đúng 1 sub: `CRAWL_REDDIT_SUBS="CaughtOnCamera"`.

### 1.4 — Rác bài ảnh/text: ĐÃ CÓ bộ lọc, nhưng chỉ khi còn cookie
`_reddit_discover_playwright()` gom mọi `href` khớp `/comments/[a-z0-9]+`
trên trang search. Pattern đó khớp cả bài text, bài ảnh, và link sang bài
khác trong sidebar — nó không có cách nào biết bài nào có video. OAuth thì
lọc được ngay tại discovery nhờ metadata (`is_video`, `post_hint` ∈
`hosted:video`/`rich:video`, `domain == 'v.redd.it'`).

Bằng chứng từ log 2026-07-29 — 1 lượt search trả 42 URL, và những URL này
đi tiếp vào pipeline:
```
ERROR: Unsupported URL: https://www.reddit.com/media?url=https%3A%2F%2Fi.redd.it%2F...jpeg
ERROR: [Reddit] 1v9obeu: No media found
```
- `Unsupported URL: .../media?url=...jpeg` ⇒ bài ẢNH (yt-dlp đi theo bài rồi
  gặp ảnh tĩnh). Không phải bug URL, là bài không có video.
- `No media found` ⇒ bài TEXT.

Thấy 2 dòng này với số lượng lớn ⇒ **đang chạy fallback Playwright**, không
phải Reddit hỏng. Cách sửa gốc là bật OAuth, không phải vá pattern regex.

### 1.5 — Nguy hiểm nhất: `[SKIP - no image]` ghi âm tính giả và L0 chặn VĨNH VIỄN
Đây là quirk quan trọng nhất của Reddit, và là lý do 9/10 URL trong DB vô
dụng. Chuỗi sự kiện, khớp chính xác với log ngày 2026-07-29:

```
1. Classifier gọi yt-dlp lấy thumbnail
      → ERROR: [Reddit] <id>: Unable to download JSON metadata: HTTP Error 429
2. Không có thumbnail → fallback chụp frame bằng Playwright  (~28 giây/URL)
      → WARNING [SKIP - no image] <url>
3. is_cctv() trả False  →  pipeline ghi status = 'skipped_not_cctv'
4. 'skipped_not_cctv' nằm trong _TERMINAL_STATUSES của L0
      → lần chạy sau URL này bị bỏ qua VĨNH VIỄN, dù nó có thể là CCTV thật
```

Đối chiếu số liệu: log có **8** dòng `[SKIP - no image]` + **1** dòng
`[SKIP - 9:16 portrait]` = đúng **9** dòng `skipped_not_cctv` trong DB.

**Hai nguyên nhân cộng dồn ở bước 1:**

1. **Reddit rate-limit (429)** — đã thấy đúng **6** lần trong 8 phút log, tức
   là chỉ sau vài URL đã bị chặn. Dấu hiệu chính xác:
   `Unable to download JSON metadata: HTTP Error 429: Too Many Requests`.
   (Đừng lẫn với `429 RESOURCE_EXHAUSTED` cũng có trong log — đó là Gemini
   hết quota, chuyện khác hẳn, xem mục 1.7.)
2. **Classifier dùng SAI cookie** — `crawler_core/qwen_classifier.py`
   hardcode `COOKIES_FILE = cookies/facebook_legacy_cookies.txt` cho CẢ
   `ydl_opts['cookiefile']` lẫn fallback `og:image`. Với URL Reddit nó gửi
   cookie **Facebook** → coi như ẩn danh. `cookies/reddit_cookies.txt` chỉ
   được dùng ở `GenericDownloader` (bước TẢI), không tới được bước thumbnail.

Phân biệt rất quan trọng, vì hai status có hậu quả trái ngược nhau:

| Triệu chứng | Status ghi vào DB | Lần chạy sau |
|---|---|---|
| `[SKIP - no image]`, `[SKIP - 9:16]`, VLM trả No | `skipped_not_cctv` | **KHÔNG làm lại** (terminal → L0 chặn) |
| `[FAILED] Không tải được`, lỗi bất ngờ | `failed` | **Làm lại** (không terminal) |

⇒ Khi đã sửa nguyên nhân gốc (bật OAuth, cookie mới), phải **dọn các URL
`skipped_not_cctv` cũ** để chúng được xử lý lại — xem mục 3. Không dọn thì
việc sửa không có tác dụng với những URL đã bị đánh dấu sai.

### 1.6 — oEmbed KHÔNG cứu được Reddit (khác Dailymotion — đừng lặp lại cách đó)
Với Dailymotion, cách chữa 401-do-rate-limit là dùng oEmbed công khai. **Với
Reddit cách đó không dùng được:** `https://www.reddit.com/oembed?url=...`
trả **HTTP 403 "Blocked"** (đã test 2026-07-31 từ chính máy này).

Vì vậy `_OEMBED_ENDPOINTS` trong `qwen_classifier.py` chỉ có `dailymotion`
và `vimeo` — thêm `reddit` vào đó là công cốc. Muốn lấy được ảnh cho Reddit
thì đường khả thi là truyền đúng cookie Reddit vào yt-dlp/og:image (mục 1.5,
điểm 2), hoặc dùng `thumbnail`/`preview` có sẵn trong JSON của OAuth ngay ở
bước discovery thay vì gọi lại yt-dlp lần nữa.

### 1.7 — Keyword do LLM sinh, ngôn ngữ NGẪU NHIÊN, và Reddit không có env override
`RedditScraper.discover()` gọi `keywords.phrases_by_label(None)` — truyền
cứng `None`, tức là **không có `CRAWL_REDDIT_KEYWORDS`**. YouTube và
Dailymotion có override riêng (`CRAWL_YOUTUBE_KEYWORDS`,
`CRAWL_DAILYMOTION_KEYWORDS`), Reddit thì không.

Mỗi lần chạy, Gemini sinh keyword với 1 ngôn ngữ chọn NGẪU NHIÊN trong 21
ngôn ngữ (`youtube_live.config.KEYWORD_LANGUAGES`), 2/3 keyword là ngôn
ngữ đó + 1/3 tiếng Anh. Search toàn Reddit khớp theo TIÊU ĐỀ bài, nên một
lượt chạy trúng tiếng Ba Tư/Ả Rập sẽ trả về đầy bài từ các sub Ả Rập không
liên quan gì tới CCTV. **Đây không phải bug**, là hệ quả của thiết kế đa ngôn
ngữ; kết quả giữa các lần chạy dao động mạnh theo ngôn ngữ trúng.

Bằng chứng nguyên văn từ log 2026-07-29 — hai dòng này giải thích trọn vẹn
vì sao lượt đó ra toàn bài Ả Rập/Ba Tư:
```
[Keywords] Gemini [Persian (Farsi)] sinh cụm từ cho 11/11 nhãn
[Keywords] Tổng 33 cụm từ cho 11 nhãn
[Discovery] https://www.reddit.com/search/?q=دوربین مداربسته ضرب و شتم&sort=new → 42 URL
```
URL thu được: `r/Xiraqis`, `r/ArabsFreedom`, `r/Egypt`, `r/SawalfMaTengal`,
`r/deardairyi`... — tiêu đề tiếng Ả Rập, nội dung không phải CCTV.

**Luôn đọc dòng `[Keywords] Gemini [<ngôn ngữ>]` đầu log trước khi kết luận
"Reddit không có video CCTV"** — nó cho biết lượt đó search bằng ngôn ngữ gì.
Cùng lượt đó Gemini còn log `429 RESOURCE_EXHAUSTED` (1 key hết quota) nhưng
vẫn xoay vòng key khác thành công → `11/11 nhãn`; thấy 429 của Gemini KHÔNG
có nghĩa là keyword đã rơi về list tĩnh, phải xem dòng `[Keywords]` mới biết
(rơi về tĩnh thì có dòng `... nhãn dùng cụm từ tĩnh`).

Cách ghim keyword khi cần tái hiện lỗi ổn định (chỉ 2 lựa chọn):
```bash
export CRAWL_LLM_KEYWORDS=false     # dùng list tĩnh trong labels.py cho MỌI platform
export CRAWL_KEYWORDS_PER_LABEL=1   # giảm số keyword/nhãn
```
Muốn override keyword riêng cho Reddit thì phải sửa code: thêm
`REDDIT_KEYWORDS_OVERRIDE = _env_list_or_none('CRAWL_REDDIT_KEYWORDS')` vào
`config.py` rồi truyền vào `phrases_by_label()` trong `RedditScraper`.

### 1.8 — Trần theo nhãn tính theo URL ĐÃ XỬ LÝ, không phải video tải được
Trong `task_crawl_platform()`: `per_label[key] += len(batch)`. Quota
`max_per_label` (mặc định 30) bị trừ cho MỌI URL đi qua pipeline, kể cả URL
bị loại vì là bài text/ảnh. Với Reddit ở chế độ fallback — nơi phần lớn URL
là rác — một nhãn có thể **cháy hết 30 quota mà tải được 0 video**, rồi log
`Nhãn X đã đủ 30/30 — bỏ qua ...` trông như đã thu đủ.

⇒ Thấy `Nhãn ... đã đủ` nhưng `downloaded=0` thì KHÔNG phải nhãn đó hết
video trên Reddit; là quota bị rác ăn hết. Sửa gốc = bật OAuth để rác không
vào pipeline ngay từ discovery.

### 1.9 — Dedup L1 của Reddit: 1 video có nhiều dạng URL
`_PLATFORM_RULES['reddit']` trong `crawler_core/dedup.py`:

| Pattern | object_type | short link? |
|---|---|---|
| `v\.redd\.it/([A-Za-z0-9]+)` | video | không |
| `/r/[^/]+/comments/([a-z0-9]+)` | post | không |
| `/comments/([a-z0-9]+)` | post | không |
| `redd\.it/([a-z0-9]+)` | post | **có** → pipeline resolve redirect TRƯỚC khi tra L1 |

Host được nhận là Reddit: `reddit.com`, `old.reddit.com`, `new.reddit.com`,
`redd.it`, `v.redd.it`.

Lưu ý: `v.redd.it/<id>` và `/comments/<post_id>` là HAI canonical ID khác
nhau cho cùng một clip (id media ≠ id bài) → L1 không bắt được cặp này.
Discovery hiện chỉ yield permalink `/comments/...` nên trong thực tế không
gặp; nhưng nếu sau này thêm nguồn yield thẳng link `v.redd.it`, trùng lặp sẽ
lọt L1 và chỉ bị L2/L3/L4 bắt **sau khi đã tải xong** (tốn băng thông). Đó
cũng là lớp bắt các clip crosspost/repost — cùng clip đăng ở nhiều sub là ID
bài khác nhau, L1 không thấy, L2/L3/L4 mới chặn.

### 1.10 — Giới hạn thời lượng: Reddit dùng mặc định 1800s
`cfg.max_duration_for('reddit')` → **1800s (30 phút)**. Chỉ Dailymotion có
override riêng (900s). Video Reddit gần như luôn ngắn nên ngưỡng này hầu như
không kích hoạt — nếu thấy log `dài {N}s > giới hạn 1800s — bỏ qua` cho URL
Reddit thì đáng nghi là link ngoài (YouTube/stream nhúng trong bài Reddit)
chứ không phải video native `v.redd.it`.

### 1.11 — Đường nào tìm được nhiều VIDEO nhất: tab Media, KHÔNG phải API
Đo thực tế cùng query `cctv robbery`, cùng cookie, 2026-07-31:

| Đường | Bài | **Có video** | Thời gian |
|---|---|---|---|
| **UI tab Media + cuộn** (Playwright) | 200 | **140 (70%)** | 86s |
| API search (`.json` / `oauth.reddit.com`) | 224 | **41 (18%)** | 5.2s |
| Trùng nhau | **chỉ 46 bài** | | |

Hai đường trả về nội dung **KHÁC HẲN NHAU**, không phải cùng tập dữ liệu chỉ
khác tốc độ: UI có riêng 154 bài (113 có video), API có riêng 178 bài nhưng
chỉ 14 có video. Tab Media tìm được **3.4× nhiều video hơn**.

⚠ **Sửa kết luận sai thứ hai của runbook này:** bản trước xếp "chuyển sang
`.json`/OAuth" là việc đáng làm số 1 vì nhanh hơn 20 lần. Sai về mục tiêu —
ta cần VIDEO, không cần tốc độ. `oauth.reddit.com/search` dùng CÙNG backend
với `.json` nên OAuth cũng kém y hệt: nhanh nhưng bỏ sót 3/4 số video.

**Kết luận đang áp dụng trong code:** `reddit_discover()` có query → luôn đi
tab Media (`_reddit_discover_media_tab`). API chỉ còn dùng cho listing
`r/<sub>/new` (không có query, không có tab Media để so).

Tham số URL ↔ nút trên UI (đã kiểm chứng từng cái):

| Nút trên UI | Tham số | API search có tôn trọng? |
|---|---|---|
| tab "Phương tiện" | `type=media` | **KHÔNG — bỏ qua hoàn toàn** |
| "Mức độ phù hợp" | `sort=relevance` | Có (cũng là mặc định) |
| "Tất cả thời gian" | `t=all` | Có (cũng là mặc định) |
| "Tắt tìm kiếm an toàn" | `include_over_18=on` | Chỉ khi cookie là tài khoản `over_18=True` |
| cuộn chuột tải thêm | (UI) `mouse.wheel` / (API) con trỏ `after` | — |

Cách chứng minh `sort`/`t` THẬT SỰ được tôn trọng (đừng thử bằng
`sort=relevance&t=all` — trùng mặc định nên kết quả y hệt, dễ kết luận nhầm
là bị bỏ qua): dùng giá trị bắt buộc phải đổi kết quả —
`sort=new` → bài hôm nay; `sort=top&t=all` → bài 2017 score 4677;
`sort=top&t=day` → chỉ 1 bài.

**Tab Media vẫn lẫn rác** — 60/200 bài không phải video (36 ảnh, 16 link
reddit.com, vài bài self). `_reddit_keep_videos()` lọc lại bằng
`/api/info.json?id=t3_a,t3_b,...` (100 ID/request, cần cookie) trước khi cho
vào pipeline. Đo sau khi có bộ lọc: 96/120 bài giữ lại, tức 80% là video thật.

Phân trang API bằng `after` (nếu vẫn cần dùng API cho việc khác): 3 trang →
224 bài unique rồi `after=None` là hết. Rate-limit đọc từ header:
`x-ratelimit-remaining/used/reset` ⇒ **100 request / 10 phút**.

### 1.12 — `sort=new`, không phân trang
Cả 2 đường (OAuth và Playwright) đều dùng `sort=new` và chỉ lấy **1 trang**:
OAuth `limit = min(limit, 100)` và KHÔNG dùng con trỏ `after` để lật trang.
Nên `CRAWL_MAX_PER_TARGET` > 100 không có thêm tác dụng gì với Reddit. Muốn
đào sâu hơn thì cách rẻ nhất là tăng số keyword/subreddit, không phải tăng
`limit`.

---

## 2. Checklist khi `downloaded=0` hoặc task chạy quá lâu

Làm theo đúng thứ tự — mỗi bước rẻ hơn bước sau rất nhiều:

1. **DAG có đang chạy không?** Reddit đang PAUSE (mục 0). Không có log
   `logs/dag_id=social_crawler_reddit/` là bình thường, không phải lỗi.
   ```bash
   airflow dags list-runs social_crawler_reddit -o plain
   ```

2. **Đang ở chế độ auth nào?** → chạy lệnh ở mục 1.2. Thấy
   `fallback Playwright` trong log ⇒ mọi triệu chứng chậm/rác/âm tính giả
   bên dưới đều là hệ quả, đừng debug riêng từng cái.

3. **Discovery có ra URL không?** Test độc lập, KHÔNG qua Airflow (nhanh hơn
   nhiều lần trigger DAG):
   ```bash
   python -c "
   import sys; sys.path.insert(0,'.')
   from dotenv import load_dotenv; load_dotenv('.env')
   from platform_crawlers.discovery import reddit_discover
   print('toàn Reddit :', len(reddit_discover(None, query='cctv robbery footage', limit=20)))
   print('r/CaughtOnCamera:', len(reddit_discover('CaughtOnCamera', query='cctv robbery footage', limit=20)))"
   ```

4. **URL ra rồi nhưng downloaded vẫn 0** → đếm loại lỗi trong log task:
   ```bash
   L="logs/dag_id=social_crawler_reddit/run_id=<run_id>/task_id=crawl_reddit/attempt=1.log"
   python - "$L" <<'PY'
   import json, sys
   from collections import Counter
   c = Counter()
   for line in open(sys.argv[1]):
       try: e = str(json.loads(line).get('event', ''))
       except Exception: continue
       for pat in ('fallback Playwright', 'OAuth OK',
                   '429: Too Many Requests',      # ⚠ Reddit rate-limit
                   'RESOURCE_EXHAUSTED',          # ⚠ KHÁC: Gemini hết quota
                   'No media found', 'Unsupported URL',
                   'SKIP - no image', 'SKIP - 9:16',
                   '[FAILED]', 'dup_url', 'đã đủ'):
           if pat in e: c[pat] += 1
   for k, v in c.most_common(): print(f'{v:5}  {k}')
   PY
   ```
   ⚠ Đừng grep chỉ `429` — trong log có **hai** loại 429 hoàn toàn khác nhau:
   `429: Too Many Requests` là Reddit chặn (mục 1.5), còn
   `429 RESOURCE_EXHAUSTED` là Gemini hết quota lúc sinh keyword (mục 1.7).
   Log 2026-07-29 có cả hai (6 của Reddit + 2 của Gemini) — gộp lại sẽ chẩn
   đoán sai hoàn toàn.

   Đọc kết quả:
   - `No media found` / `Unsupported URL` nhiều → bài text/ảnh, do fallback (mục 1.4)
   - `429: Too Many Requests` → Reddit rate-limit lúc lấy thumbnail (mục 1.5)
   - `RESOURCE_EXHAUSTED` → Gemini hết quota; keyword có thể rơi về list tĩnh (mục 1.7)
   - `SKIP - no image` nhiều → ÂM TÍNH GIẢ, sẽ bị L0 chặn vĩnh viễn (mục 1.5)
   - `đã đủ` nhưng downloaded=0 → quota nhãn bị rác ăn hết (mục 1.8)
   - `dup_url` nhiều → đã xử lý từ lần trước; muốn làm lại xem mục 3

5. **Task chạy hàng giờ chưa xong** → bình thường khi chưa có OAuth (198 lượt
   Playwright, mục 1.3). Muốn smoke-test nhanh thì dùng time budget thay vì
   đợi hoặc kill:
   ```bash
   airflow dags trigger social_crawler_reddit \
     --conf '{"max_per_label": 3, "max_per_platform": 10, "time_budget_minutes": 10}'
   ```
   Task tự dừng ĐÚNG LỊCH giữa các batch/nguồn, không bị SIGKILL giữa lúc
   đang tải (kill cứng để lại file dở + ffmpeg mồ côi).

6. **Log kết thúc bằng `Server indicated the task shouldn't be running anymore`
   / `Task killed!`** → task bị dừng từ BÊN NGOÀI (clear/mark-failed trên UI,
   hoặc DAG run bị xóa), KHÔNG phải crash trong code. Đây chính là cách run
   2026-07-29 kết thúc sau 8 phút — nên đừng tìm bug ở dòng cuối cùng nó
   đang xử lý.

7. **Trước khi nghi VLM sai** — mở đúng URL đó trong browser và xem thật.
   Với Reddit thì khả năng cao hơn là *không lấy được ảnh nào để phân loại*
   (mục 1.5), chứ không phải VLM nhìn sai ảnh.

---

## 3. Truy vấn DB nhanh (bảng dùng chung, lọc theo `reddit`)

DB: `data/db/tracker.db` (đường dẫn cũ, dùng chung mọi platform).

```bash
python -c "
import sqlite3
c = sqlite3.connect('data/db/tracker.db')
print('sources (nguồn đã scan):',
      c.execute('SELECT COUNT(*) FROM sources WHERE source_type=\"reddit\"').fetchone()[0])
print('video_urls theo status:')
for r in c.execute('SELECT status, COUNT(*) FROM video_urls WHERE source_platform=\"reddit\" GROUP BY status'):
    print(' ', r)
print('fingerprints (video unique đã tải):',
      c.execute('SELECT COUNT(*) FROM video_fingerprints WHERE source_platform=\"reddit\"').fetchone()[0])
"
```

**Dọn âm tính giả** — bắt buộc sau khi sửa nguyên nhân gốc ở mục 1.5, nếu
không thì URL đã bị đánh dấu sai sẽ bị L0 bỏ qua mãi mãi:
```bash
# 1. Backup TRƯỚC (luôn luôn)
python -c "
import sqlite3, json
c = sqlite3.connect('data/db/tracker.db')
rows = c.execute('SELECT * FROM video_urls WHERE source_platform=\"reddit\"').fetchall()
cols = [d[0] for d in c.execute('SELECT * FROM video_urls LIMIT 0').description]
json.dump([dict(zip(cols, r)) for r in rows],
          open('/tmp/reddit_video_urls_backup.json','w'), indent=2, ensure_ascii=False)
print(len(rows), 'dòng đã backup')"

# 2. Chỉ xóa các URL bị loại vì KHÔNG LẤY ĐƯỢC ẢNH (giữ nguyên downloaded/
#    skipped_duplicate — chúng là kết luận đúng, xóa đi sẽ tải trùng lại)
python -c "
import sqlite3
c = sqlite3.connect('data/db/tracker.db')
n = c.execute('DELETE FROM video_urls WHERE source_platform=\"reddit\" '
              'AND status=\"skipped_not_cctv\"').rowcount
c.commit(); print(n, 'dòng skipped_not_cctv đã xóa')"
```
Lưu ý: `skipped_not_cctv` gộp CẢ 3 nguyên nhân (no-image, 9:16 portrait, VLM
trả No) nên lệnh trên cũng xóa luôn các kết luận đúng — chấp nhận được vì
Reddit chỉ có 9 dòng và tất cả đều đáng xử lý lại. Với platform có nhiều dữ
liệu thì cân nhắc kỹ hơn, xóa `video_urls` KHÔNG xóa `video_fingerprints`
nên video đã tải vẫn được bảo vệ khỏi tải trùng (bị L2/L3/L4 bắt sau khi tải
lại — vẫn tốn băng thông).

---

## 4. Vận hành Airflow — LƯU Ý QUAN TRỌNG VỀ AIRFLOW_HOME

Máy này có **3 giá trị AIRFLOW_HOME khác nhau** dễ gây nhầm lẫn. Giá trị đúng:
```bash
export AIRFLOW_HOME="$(pwd)"   # chạy từ thư mục gốc dự án — KHÔNG có hậu tố /airflow
```
Nếu scheduler đang chạy, xác minh bằng process thật trước khi tin CLI:
```bash
ps aux | grep -E 'airflow (scheduler|dag-processor)' | grep -v grep
tr '\0' '\n' < /proc/<PID>/environ | grep AIRFLOW_HOME
```
(Kiểm tra 2026-07-31: KHÔNG có scheduler nào đang chạy — khởi động bằng
`./start_airflow.sh` trước khi mong DAG tự chạy theo lịch.)

```bash
# Bật DAG (đang PAUSE)
airflow dags unpause social_crawler_reddit

airflow dags list-runs social_crawler_reddit -o plain   # dag_id là POSITIONAL, không phải --dag-id
airflow tasks clear social_crawler_reddit -t crawl_reddit -s <date> -e <date> -d -y
airflow dags trigger social_crawler_reddit --conf '{"max_per_label": 5, "time_budget_minutes": 10}'
```

Log task nằm ở:
```
logs/dag_id=social_crawler_reddit/run_id=<run_id>/task_id=crawl_reddit/attempt=N.log
```
Log là JSON mỗi dòng 1 record — dùng `json.loads` rồi đọc field `event`, đừng
grep thô (mục 2 bước 4 có script sẵn).

---

## 5. Pool GPU dùng chung

Task này chạy trong pool `social_crawler_gpu` (1 slot) — chia sẻ với 6 DAG
platform khác (`social_crawler_youtube/tiktok/instagram/x/dailymotion/vimeo`)
để tránh 2 platform cùng nạp Qwen2.5-VL lên GPU gây OOM. Reddit là platform
CHẬM NHẤT khi chưa có OAuth (mục 1.3) nên nó giữ slot rất lâu và làm các
platform khác "queued" theo. Kiểm tra:
```bash
airflow pools list
```
Pool chỉ được dùng khi `SOCIAL_USE_CLASSIFIER=true`; tắt classifier thì task
chạy ở pool default (không cần GPU).

---

## 6. Nếu định sửa code — xếp theo giá trị/công sức

Không sửa gì cũng được nếu chỉ cần vận hành, nhưng khi được yêu cầu cải
thiện thì đây là thứ tự đáng làm, kèm lý do:

1. **Cho `RedditScraper.discover()` biết trần `max_per_label`** — lãng phí
   lớn nhất còn lại: ~17/18 lượt discovery mỗi nhãn chạy xong rồi bị bỏ vì
   quota đã đầy (mục 1.3), tốn ~2.8h/run. Scraper tự đếm URL đã yield theo
   nhãn và bỏ qua nguồn còn lại của nhãn đó.
2. **Truyền cookie ĐÚNG PLATFORM vào classifier** — `qwen_classifier.py`
   đang hardcode `facebook_legacy_cookies.txt` cho mọi platform (mục 1.5). Dùng
   `crawler_core.downloader.cookies_file_for(platform)` thay cho hằng
   `COOKIES_FILE`. Ảnh hưởng TỐT cho cả X/TikTok/Instagram, không riêng Reddit.
4. **Dùng luôn `thumbnail`/`preview` có trong JSON** — discovery đã có sẵn
   metadata trong tay, đang bỏ đi rồi bắt classifier gọi lại yt-dlp (chỗ bị
   429). Cần đổi chữ ký để `reddit_discover()` trả kèm thumbnail. Áp dụng cho
   cả đường `.json` và OAuth vì hai bên cùng cấu trúc dữ liệu.
5. **Thêm `CRAWL_REDDIT_KEYWORDS`** (mục 1.7) — nhỏ, giúp tái hiện lỗi ổn định.
6. **Hạ `execution_timeout` về 3h** sau khi discovery không còn dùng Playwright
   (mục 1.3) — 4h chỉ tồn tại vì 198 lượt mở browser.

---

## 7. Trước khi kết luận "đây là bug mới"

Thứ tự điều tra đã chứng minh hiệu quả với các DAG platform khác:
1. Đọc log task thật, đừng đoán từ mô tả. Với Reddit, xác định **chế độ auth**
   ở dòng log đầu tiên của discovery trước khi đọc tiếp — nó quyết định ý
   nghĩa của mọi dòng sau.
2. Query DB trực tiếp xem trạng thái thật (đừng tin log tóm tắt "lũy kế").
3. Tái hiện bằng script Python độc lập, KHÔNG qua Airflow.
4. Nghi Playwright/discovery → mở page thật, in text/DOM ra xem CHÍNH XÁC
   trang trả về gì. Với Reddit nhớ rằng HTML thô chỉ ~8KB vỏ JS (mục 1.1),
   nên `requests` "không thấy gì" là bình thường, không phải bằng chứng bị chặn.
5. Phân biệt **"Reddit chặn"** với **"đang chạy chế độ kém"**: 403 trên
   `.json` là chính sách cố định của Reddit từ 2023, KHÔNG phải sự cố mới
   và KHÔNG sửa được bằng cách đổi User-Agent/subdomain — đã test 4 cách.
6. Khi user nghi ngờ một kết luận trước đó, ĐỪNG bảo vệ kết luận cũ — kiểm
   chứng lại nghiêm ngặt hơn bằng bằng chứng MỚI (chạy lại request thật, đếm
   lại trong log, query lại DB), rồi mới trả lời.
