---
name: fb-dag
description: Debug, vận hành và bảo trì DAG social_crawler_fb (Facebook search tab "Thước phim") — cookie facebook_legacy_cookies.txt (DAG cũ) bị yt-dlp ghi đè mất login, thiếu cookie thì ERR_CONNECTION_REFUSED chứ không hiện trang login, ID video nằm trong query string nên không dùng chung playwright_discover được, VideoDownloader KHÔNG có giới hạn thời lượng/dung lượng. Dùng khi DAG này ra 0 URL, tải hỏng, hoặc cần nhập lại cookie Facebook.
---

# social_crawler_fb — Runbook

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

DAG crawl video CCTV từ Facebook theo LUỒNG SEARCH, đi qua cổng dedup 4 tầng
dùng chung (`crawler_core.pipeline.VideoPipeline`). Tạo ngày 2026-07-31.

| File | Vai trò |
|---|---|
| `dags/social_crawler_fb_dag.py` | Định nghĩa DAG — `dag_id='social_crawler_fb'`, schedule `ROTATION_SCHEDULE` (10 phút, xoay vòng liên tục qua pool 1 slot — xem mục "Chạy TUẦN TỰ LIÊN TỤC" trong `social_crawler_common.py`), `execution_timeout=4h` |
| `dags/social_crawler_common.py` | `task_crawl_platform()` — chứa **nhánh riêng cho facebook** chọn `VideoDownloader`, xem mục 1.4 |
| `platform_crawlers/scrapers.py` → `FacebookScraper` | Sinh query theo 11 nhãn, gọi `facebook_search_discover()`. `needs_cookies = True` |
| `platform_crawlers/discovery.py` → `facebook_search_discover()` | **Hàm riêng**, KHÔNG dùng chung `playwright_discover()` — lý do ở mục 1.3 |
| `platform_crawlers/config.py` | `FACEBOOK_KEYWORDS_OVERRIDE`, `FACEBOOK_MAX_SCROLLS` (mặc định = `MAX_SCROLLS` = 60), `DISCOVERY_TIMEOUT` (600s) |
| `crawler_core/dedup.py` → `_PLATFORM_RULES['facebook']` | Canonical ID (L1): `/reel/<id>` (ĐỨNG TRƯỚC video), `?v=<id>`, `/videos/<id>`, `fb.watch/<id>`… |
| `crawler_core/facebook_downloader.py` | `VideoDownloader` — yt-dlp trước, không được thì bắt DASH segment qua Playwright |
| `cookies/facebook_cookies.json` / `.txt` | Cookie ĐÚNG cho DAG này (json→Playwright, txt→tải) |
| ⚠ `cookies/facebook_legacy_cookies.txt` | **KHÔNG dùng** — file hỏng, xem mục 1.2 |
| ⚠ `dags/legacy_fb_groups_crawler_dag.py` | **DAG KHÁC** (quét nhóm, không search) — xem mục 1.9 |

---

## 0. Trạng thái đã kiểm chứng (2026-07-31)

| Hạng mục | Trạng thái | Hệ quả |
|---|---|---|
| DAG `social_crawler_fb` | Mới tạo, **đang PAUSE**, chưa có run nào | — |
| Cookie | `cookies/facebook_cookies.{json,txt}` — 12 cookie có `c_user`+`xs`, nhập từ Chrome Default | Discovery + tải đều chạy được |
| Chuỗi xử lý | **Đã chạy thật end-to-end**: discover 40 URL/từ khóa → pipeline → tải 1.3 MB/3s → `downloaded=1` | Toàn tuyến thông |
| DB | `video_urls`/`fingerprints`/`sources` cho `facebook` đều **= 0** (dữ liệu test đã dọn) | Chưa có dữ liệu thật |
| Dữ liệu FB cũ | 4749 dòng `video_urls` `source_platform IS NULL` (DAG cũ), **0/4749 file còn trên đĩa** | L0 vẫn chặn theo URL; không dựng lại fingerprint được |

⚠ **Cảnh báo lịch sử:** ngày 2026-07-31, 40 dòng `video_fingerprints` của
Facebook (id 1–40) **đã bị xóa nhầm** bằng lệnh dọn
`DELETE ... WHERE source_platform='facebook'`. Không khôi phục được (không
WAL/journal, DB không trong git, file nguồn đã mất hết). Hệ quả: 40 clip đó
mất bảo vệ L1/L2/L3/L4 — nếu gặp lại ở URL khác sẽ bị tải trùng; L0 vẫn chặn
nếu trùng đúng URL. **Bài học: khi dọn dữ liệu test, lọc theo `id >` mốc đã
ghi trước đó, TUYỆT ĐỐI không lọc theo `source_platform`** (bảng có thể đã
chứa dữ liệu cũ của cùng platform từ DAG khác).

---

## 1. Quirks đặc thù của Facebook (đã kiểm chứng thực tế)

### 1.1 — "Thước phim" chính là tab `/search/videos/`
Không phải đoán từ tên. Đọc trực tiếp href của các tab trên trang search
(UI tiếng Việt):

| Nhãn trên UI | URL |
|---|---|
| Tất cả | `/search/top/` |
| Mọi người | `/search/people/` |
| **Thước phim** | **`/search/videos/`** |
| Trang | `/search/pages/` |
| Nhóm | `/search/groups/` |
| Sự kiện | `/search/events/` |

Facebook đặt nhãn tiếng Việt "Thước phim" cho tab Videos. Tự kiểm chứng lại
khi FB đổi giao diện:
```bash
python -c "
import json, re
from playwright.sync_api import sync_playwright
from urllib.parse import quote
UA=('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36')
with sync_playwright() as pw:
    b=pw.chromium.launch(headless=True,args=['--no-sandbox'])
    ctx=b.new_context(user_agent=UA,viewport={'width':1360,'height':950},locale='vi-VN')
    ctx.add_cookies(json.load(open('cookies/facebook_cookies.json')))
    p=ctx.new_page()
    p.goto('https://www.facebook.com/search/top/?q='+quote('cctv'),
           wait_until='domcontentloaded',timeout=60000)
    p.wait_for_timeout(9000)
    seen=set()
    for a in p.query_selector_all('a[href*=\"/search/\"]'):
        m=re.search(r'/search/([a-z_]+)/', a.get_attribute('href') or '')
        if m and m.group(1) not in seen:
            seen.add(m.group(1)); print(f'{(a.inner_text() or \"\").strip()[:20]:22} → /search/{m.group(1)}/')
    b.close()"
```

### 1.2 — `cookies/facebook_legacy_cookies.txt` BỊ HỎNG — đừng dùng
File cookie "chính thống" của crawler FB cũ đã bị phá. Nội dung thực tế
kiểm tra ngày 2026-07-31:

```
header : "# This file is generated by yt-dlp.  Do not edit."
domain : .dailymotion.com(3) .facebook.com(5) .instagram.com(7)
         .reddit.com(4) .x.com(5) .youtube.com(7)  ← LẪN 6 PLATFORM
cookie facebook còn lại: datr, fr, ps_l, ps_n, sb   ← toàn cookie tracking
c_user / xs: KHÔNG CÓ                                ← mất login
```

**Nguyên nhân:** `crawler_core/qwen_classifier.py:25` hardcode
`COOKIES_FILE = cookies/facebook_legacy_cookies.txt` rồi dùng làm
`ydl_opts['cookiefile']` cho **MỌI platform**. yt-dlp ghi ngược cả cookie jar
vào file đó sau mỗi lần chạy → cookie của Dailymotion/Reddit/X/YouTube tràn
vào và đè mất login Facebook.

⇒ Hệ quả kép:
- DAG này **né hẳn** file đó, dùng `cookies/facebook_cookies.{json,txt}` riêng.
- **DAG cũ `legacy_fb_groups_crawler` đang chạy với cookie FB hỏng** (nó vẫn trỏ vào
  file gốc). Chưa sửa — sửa `_COOKIE_PATHS['facebook']` sẽ đổi hành vi DAG cũ,
  và chừng nào classifier còn ghi đè thì fix cũng bị phá lại. Muốn sửa gốc
  thì phải cho classifier dùng `cookies_file_for(platform)` thay vì hằng số.

### 1.3 — KHÔNG dùng chung `playwright_discover()` được: ID nằm trong query
Link kết quả của tab Thước phim có dạng:
```
/watch/?ref=search&v=1582385296612845&external_log_id=...&q=cctv
                    └── ID video nằm ở ĐÂY, trong query string
```
Mà `playwright_discover()` có dòng `full = full.split('?')[0]`
(`platform_crawlers/discovery.py`) → cắt sạch query → mọi URL biến thành
`https://www.facebook.com/watch/` rỗng, mất hết ID.

Vì vậy có hàm riêng `facebook_search_discover()`. Nó bắt **3 dạng href**
(đếm thực tế trên 1 query: **598** lượt khớp `?v=` so với **18** lượt
`/videos/`):

| Dạng href | Loại | URL canonical dựng lại |
|---|---|---|
| `/watch/?...&v=<id>` | video | `https://www.facebook.com/watch/?v=<id>` |
| `/<user>/videos/<id>/` | video | `https://www.facebook.com/watch/?v=<id>` |
| `/reel/<id>` | reel | `https://www.facebook.com/reel/<id>` |

Đã kiểm chứng dedup nhận đúng:
```
watch/?v=10154414037989202                → ('facebook','video','10154414037989202')
reel/1234567890                           → ('facebook','reel','1234567890')
tu.quy.536275/videos/2498077680477253/    → ('facebook','video','2498077680477253')
```

### 1.4 — Facebook dùng downloader RIÊNG, và nó KHÔNG có giới hạn nào
`task_crawl_platform()` có nhánh riêng: platform `facebook` → `VideoDownloader`
(yt-dlp trước, thất bại thì bắt DASH segment qua Playwright rồi mux), mọi
platform khác → `GenericDownloader`.

⚠ **Khác biệt quan trọng về giới hạn:**

| | `GenericDownloader` | `VideoDownloader` (Facebook) |
|---|---|---|
| `max_duration` | 1800s, chặn TRƯỚC khi tải | **KHÔNG CÓ** |
| `max_filesize_mb` | 500 MB | **KHÔNG CÓ** |

`cfg.max_duration_for('facebook')` vẫn trả 1800 nhưng **không ai truyền nó
vào** — chữ ký `VideoDownloader.__init__(output_base, cookies_file)` không
nhận tham số đó. Nghĩa là một video FB dài 2 tiếng sẽ được tải TRỌN VẸN.
Nếu thấy đĩa đầy nhanh bất thường, đây là chỗ đầu tiên cần nhìn.

Điểm tốt: `VideoDownloader.download()` luôn chạy `ffprobe` kiểm tra file có
video stream đọc được không, không có thì xóa — nên không để lại file rác.

### 1.5 — Thiếu cookie: `ERR_CONNECTION_REFUSED`, KHÔNG phải trang login
Tái hiện **3/3 lần** ngày 2026-07-31 (cùng lúc đó chạy CÓ cookie vẫn ra kết
quả bình thường, nên không phải FB chặn IP):
```
Playwright, KHÔNG cookie → net::ERR_CONNECTION_REFUSED at facebook.com/search/videos/
Playwright, CÓ cookie    → 10 URL bình thường
```
⇒ Guard `if 'login' in page.url or 'checkpoint' in page.url` trong
`facebook_search_discover()` **không bao giờ kích hoạt cho trường hợp này** —
`page.goto()` ném exception trước, rơi vào `except` và log:
```
[Discovery] Facebook Playwright lỗi (<query>): ... ERR_CONNECTION_REFUSED
```
Thấy dòng đó ⇒ nghĩ tới cookie TRƯỚC TIÊN, đừng đi tìm lỗi mạng. Guard
login/checkpoint vẫn giữ vì nó bắt trường hợp khác (cookie có nhưng bị
checkpoint).

Ghi chú: `requests` thuần trả **HTTP 400** cho trang search dù có hay không
cookie — FB đòi browser thật. Đừng dùng `requests` để chẩn đoán.

### 1.6 — Cuộn có tác dụng rõ rệt và chưa chạm đáy ở 15 lần
Đo thực tế trên query `cctv cướp giật`:

| Số lần cuộn | 1 | 2 | 4 | 6 | 8 | 10 | 12 |
|---|---|---|---|---|---|---|---|
| ID unique | 14 | 21 | 35 | 49 | 63 | 77 | **91** |

Tăng gần như tuyến tính (~7 ID/lần cuộn), chưa có dấu hiệu cạn. Mặc định giờ là
`FACEBOOK_MAX_SCROLLS=60` (2026-07-31, trước là 15); mỗi lần cuộn tốn ~2.5s.

⚠ **Tăng `FACEBOOK_MAX_SCROLLS` một mình KHÔNG có tác dụng gì.** Vòng cuộn dừng
ở `if len(found) >= limit: break`, mà `limit` = `CRAWL_MAX_PER_TARGET` — phải
tăng cái đó TRƯỚC (đã đặt 100). Hàm còn tự dừng khi 3 vòng liên tiếp không ra ID
mới (= đã tới đáy thật) hoặc hết `CRAWL_DISCOVERY_TIMEOUT`.

✅ **Bug quá hạn đã sửa (2026-07-31).** Trước đây hàm bọc trong
`with ThreadPoolExecutor(...) as ex: ... result(timeout=420)`. `__exit__` gọi
`shutdown(wait=True)` nên nó **vẫn chờ thread chạy xong bất chấp timeout**, rồi
`return []` — mất trắng toàn bộ ID đã gom. Đã đo: `timeout=1s` với hàm `sleep(3)`
thực tế chờ 3s. Nay dùng accumulator ngoài `_run()` + `shutdown(wait=False)`:
quá hạn thì trả về phần đã gom. Kiểm chứng: cuộn Wikipedia với `timeout_s=8` →
trả về **490 URL sau đúng 8.0s** thay vì 0 URL sau ~150s.

### 1.7 — Quy mô và tốc độ 1 run
```
11 nhãn × 3 keyword = 33 lượt search
~22 giây/lượt (đo thật: 2 từ khóa hết 44s, mỗi lượt 40 URL)
→ discovery ≈ 12 phút    (nhanh hơn Reddit 198 lượt/~3h rất nhiều)
```
`execution_timeout` đặt 4h vì phần TẢI mới là chỗ tốn: video FB không phải
lúc nào cũng tải được bằng yt-dlp, phải rơi về bắt DASH segment qua
Playwright — chậm hơn nhiều lần.

### 1.8 — Lọc 9:16 đang BẬT, mà tab Thước phim có cả Reels
`facebook` không nằm trong `ALLOW_PORTRAIT` ⇒ `skip_portrait=True`. Reels FB
gần như luôn 9:16 và sẽ bị loại **sau khi đã tải xong** (tốn băng thông rồi
mới xóa). Discovery có bắt cả `/reel/<id>` nên chúng CÓ vào pipeline.

Muốn giữ clip dọc: `export CRAWL_ALLOW_PORTRAIT="facebook"`.
Muốn khỏi tốn băng thông: bỏ dạng `/reel/` khỏi `_FB_ID_PATTERNS` trong
`discovery.py` để chúng không vào pipeline ngay từ đầu.

### 1.9 — Hai DAG Facebook, làm việc khác nhau
| | `social_crawler_fb` (mới) | `legacy_fb_groups_crawler` (cũ) |
|---|---|---|
| File | `dags/social_crawler_fb_dag.py` | `dags/legacy_fb_groups_crawler_dag.py` |
| Nguồn | **SEARCH** theo 33 từ khóa của 11 nhãn | Quét video trong các **NHÓM** đã tham gia |
| Task | `crawl_facebook` | `crawl_all_groups`, `report_stats` |
| Cookie | `cookies/facebook_cookies.*` (sạch) | `cookies/facebook_legacy_cookies.txt` (**hỏng**, mục 1.2) |
| Gán nhãn | Có (theo keyword) | Không |

Cả hai đều đẩy qua `VideoPipeline` và dùng chung fingerprint store, nên video
trùng giữa hai đường bị L1/L2/L3/L4 chặn — chạy song song không sợ tải 2 lần
(miễn là fingerprint còn nguyên, xem cảnh báo ở mục 0).

---

## 2. Checklist khi `downloaded=0` / discovery ra 0 URL

1. **Cookie còn sống không?** — phép thử chức năng, đúng việc crawler làm:
   ```bash
   python -c "
   import sys; sys.path.insert(0,'.')
   from platform_crawlers.discovery import facebook_search_discover
   u = facebook_search_discover('cctv robbery', limit=10,
                                cookies_json='cookies/facebook_cookies.json',
                                max_scrolls=2)
   print(len(u), 'URL'); print(u[:2])"
   ```
   Ra 0 + log `ERR_CONNECTION_REFUSED` ⇒ cookie hỏng/thiếu (mục 1.5) →
   `python scripts/import_browser_cookies.py facebook`.
2. **Nhập cookie xong vẫn 0?** Kiểm tra profile Chrome nào đang đăng nhập FB:
   ```bash
   python scripts/import_browser_cookies.py --list | grep -A5 chrome
   ```
   Nhớ `--list` chỉ so TÊN cookie, không biết phiên còn sống — vẫn phải chạy
   phép thử ở bước 1.
3. **Có URL nhưng `downloaded=0`?** Xem log tìm:
   - `yt-dlp thất bại, thử Playwright` → đang rơi về DASH capture, chậm nhưng
     bình thường
   - `[Validate] File không có video stream playable — xóa` → tải ra file hỏng
   - `[SKIP - 9:16 portrait]` → Reels bị lọc, mục 1.8
   - `dup_url` → đã xử lý lần trước (L0)
4. **Đĩa đầy nhanh?** → mục 1.4, `VideoDownloader` không giới hạn thời lượng
   lẫn dung lượng.
5. **Task kẹt `queued`?** → slot pool `social_crawler_gpu` bị task khác giữ:
   ```bash
   airflow pools list
   ```

---

## 3. Truy vấn DB nhanh

```bash
python -c "
import sqlite3
c = sqlite3.connect('data/db/tracker.db')
print('sources     :', c.execute('SELECT COUNT(*) FROM sources WHERE source_type=\"facebook\"').fetchone()[0])
print('video_urls theo status:')
for r in c.execute('SELECT status, COUNT(*) FROM video_urls WHERE source_platform=\"facebook\" GROUP BY status ORDER BY 2 DESC'):
    print('   ', r)
print('fingerprints:', c.execute('SELECT COUNT(*) FROM video_fingerprints WHERE source_platform=\"facebook\"').fetchone()[0])
print()
print('ĐÃ TẢI theo nhãn:')
for r in c.execute('SELECT final_category, COUNT(*) FROM video_urls '
                   'WHERE source_platform=\"facebook\" AND status=\"downloaded\" '
                   'GROUP BY final_category ORDER BY 2 DESC'):
    print('   ', r)
"
```
Tên cột dễ nhầm: `video_urls` có `final_category`/`category_hint` (KHÔNG có
`category`); `sources` có `source_type`/`category` (KHÔNG có `source_name`).

**Dữ liệu FB cũ nằm ở `source_platform IS NULL`** (4749 dòng downloaded) —
DAG cũ ghi vậy. Đếm riêng nếu cần:
```bash
python -c "
import sqlite3
c = sqlite3.connect('data/db/tracker.db')
print(c.execute(\"SELECT COUNT(*) FROM video_urls WHERE source_platform IS NULL \"
                \"AND status='downloaded'\").fetchone()[0], 'video FB cũ (legacy)')"
```

⚠ **Khi dọn dữ liệu test, KHÔNG lọc theo `source_platform`** — xem cảnh báo
ở mục 0. Cách an toàn: ghi lại `MAX(id)` trước khi test rồi xóa theo
`id > <mốc>`:
```bash
# TRƯỚC khi test
python -c "
import sqlite3; c=sqlite3.connect('data/db/tracker.db')
print('mốc id:', c.execute('SELECT MAX(id) FROM video_fingerprints').fetchone()[0])"
# SAU khi test — thay <mốc> bằng số ở trên
python -c "
import sqlite3; c=sqlite3.connect('data/db/tracker.db')
print(c.execute('DELETE FROM video_fingerprints WHERE id > <mốc>').rowcount, 'dòng đã xóa'); c.commit()"
```

---

## 4. Vận hành Airflow

```bash
export AIRFLOW_HOME="$(pwd)"   # chạy từ thư mục gốc dự án — KHÔNG có hậu tố /airflow

airflow dags unpause social_crawler_fb        # đang PAUSE
airflow dags list-runs social_crawler_fb -o plain    # dag_id POSITIONAL
airflow tasks clear social_crawler_fb -t crawl_facebook -s <date> -e <date> -d -y

# Chạy thử nhanh, không tải nhiều:
airflow dags trigger social_crawler_fb \
  --conf '{"max_per_label": 3, "max_per_platform": 10, "time_budget_minutes": 10}'
```
⚠ `dag_id` là `social_crawler_fb`, còn **file** là `social_crawler_fb_dag.py`
và **task** là `crawl_facebook` (theo tên platform, không phải `fb`).

Log: `logs/dag_id=social_crawler_fb/run_id=<run_id>/task_id=crawl_facebook/attempt=N.log`
(JSON mỗi dòng — `json.loads` rồi đọc field `event`).

---

## 5. Pool GPU dùng chung

Task chạy trong pool `social_crawler_gpu` (1 slot), chia sẻ với 7 DAG platform
khác. Kiểm tra ai đang giữ slot trước khi trách DAG không chạy:
```bash
airflow pools list
python -c "
import sqlite3
c = sqlite3.connect('airflow.db')
for r in c.execute(\"SELECT dag_id,task_id,state,start_date FROM task_instance \"
                   \"WHERE pool='social_crawler_gpu' AND state IN ('running','queued')\"):
    print(r)"
```
(2026-07-31: slot đang bị task zombie `crawl_x` giữ — task `state=running`
nhưng không có tiến trình airflow nào tồn tại.)

---

## 6. Nếu định sửa code — xếp theo giá trị/công sức

1. **Truyền giới hạn thời lượng/dung lượng cho `VideoDownloader`** (mục 1.4) —
   hiện video FB dài bao nhiêu cũng tải trọn. Thêm `max_duration`/
   `max_filesize_mb` vào `__init__` rồi đưa vào `ydl_opts` giống
   `GenericDownloader._build_match_filter()`.
2. **Cho `qwen_classifier.py` dùng `cookies_file_for(platform)`** thay hằng
   `COOKIES_FILE` (mục 1.2) — sửa gốc việc cookie FB bị ghi đè, đồng thời
   giúp cả Reddit/X/TikTok lấy được thumbnail bằng đúng cookie của mình.
3. **Bỏ `/reel/` khỏi `_FB_ID_PATTERNS`** nếu không muốn tốn băng thông tải
   Reels rồi xóa vì 9:16 (mục 1.8).
4. **Log rõ hơn khi thiếu cookie** — hiện chỉ ra `Facebook Playwright lỗi ...
   ERR_CONNECTION_REFUSED` (mục 1.5); bắt riêng lỗi này và gợi ý luôn lệnh
   nhập cookie sẽ đỡ mất thời gian chẩn đoán.

---

## 7. Trước khi kết luận "đây là bug mới"

1. **Chạy phép thử cookie ở mục 2 bước 1 trước tiên.** Gần như mọi triệu
   chứng "0 URL" của Facebook đều quy về cookie.
2. Xác định đúng DAG (mục 1.9) — `legacy_fb_groups_crawler` cũ quét NHÓM, không search,
   và nó đang dùng file cookie hỏng.
3. Tái hiện bằng script Python độc lập, KHÔNG qua Airflow — `facebook_search_discover()`
   gọi trực tiếp được, ~22s là có kết quả.
4. Nghi Playwright → mở page thật, in `p.url`, `p.title()`,
   `p.inner_text('body')[:300]`. Với FB nhớ dùng `locale='vi-VN'` nếu muốn
   nhãn tab khớp tài liệu này.
5. **Đừng dùng `requests` để chẩn đoán FB** — luôn trả HTTP 400 dù cookie
   đúng hay sai, không phân biệt được gì (mục 1.5).
6. Đừng mang kinh nghiệm platform khác sang: FB KHÔNG dùng chung
   `playwright_discover()` (mục 1.3), KHÔNG dùng `GenericDownloader`
   (mục 1.4), và cookie của nó nằm ở chỗ khác với file "chính thống" cũ
   (mục 1.2).
