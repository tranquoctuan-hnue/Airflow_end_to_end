---
name: youtube-dag
description: Debug, vận hành và bảo trì DAG social_crawler_youtube — platform DUY NHẤT đang thực sự tải được video (136 clip), nhưng chỉ 5/11 nhãn có dữ liệu vì task chết trước khi tới nhãn sau; age-gate chiếm 59% lỗi tải; dễ nhầm với DAG camera_source_youtube_live; GPU pool có thể bị task zombie chiếm. Dùng khi DAG này lỗi, task kẹt queued, dataset lệch nhãn, hoặc downloaded thấp bất thường.
---

# social_crawler_youtube — Runbook

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

DAG crawl video CCTV từ YouTube, đi qua cổng dedup 4 tầng dùng chung
(`crawler_core.pipeline.VideoPipeline`). File chính:

| File | Vai trò |
|---|---|
| `dags/social_crawler_youtube_dag.py` | Định nghĩa DAG — schedule `ROTATION_SCHEDULE` (10 phút, xoay vòng liên tục qua pool 1 slot — xem mục "Chạy TUẦN TỰ LIÊN TỤC" trong `social_crawler_common.py`), KHÔNG override gì nên dùng mặc định: `execution_timeout=3h`, `retries=1`, `retry_delay=15m` |
| `dags/social_crawler_common.py` | `task_crawl_platform()` — logic chạy chung, chứa trần `max_per_label`/`max_per_platform`/time budget |
| `platform_crawlers/scrapers.py` → `YouTubeScraper` | Đơn giản nhất trong các scraper: `ytsearch<N>:<keyword>`, KHÔNG cần cookie, KHÔNG dùng Playwright |
| `platform_crawlers/discovery.py` → `ytdlp_discover()` | Liệt kê URL bằng yt-dlp `extract_flat`, bỏ live stream ngay ở bước này |
| `platform_crawlers/discovery.py` → `youtube_search_target()` | Sinh target `ytsearch{limit}:{keyword}` |
| `platform_crawlers/config.py` | `YOUTUBE_KEYWORDS_OVERRIDE` (`CRAWL_YOUTUBE_KEYWORDS`) |
| `crawler_core/dedup.py` → `_PLATFORM_RULES['youtube']` | Canonical ID (L1): `/shorts/<id>`, `?v=<id>`, `youtu.be/<id>`, `/embed/<id>`, `/live/<id>` |
| `crawler_core/downloader.py` → `_COOKIE_PATHS['youtube']` | `cookies/youtube_cookies.txt` — **hiện KHÔNG tồn tại**, xem mục 1.2 |
| ⚠ `dags/camera_source_youtube_live_dag.py` | **DAG KHÁC**, không liên quan — xem mục 1.4 để khỏi nhầm |

---

## 0. Trạng thái đã kiểm chứng (2026-07-31) — đọc trước khi debug

| Hạng mục | Trạng thái thực tế | Hệ quả |
|---|---|---|
| Kết quả trong DB | **136 downloaded, 136 fingerprint**, 50 `skipped_not_cctv`, 22 `failed`, 3 `pending`, 9 sources | **Platform DUY NHẤT thực sự hoạt động** (Reddit 0, X 0, Dailymotion/Instagram chưa có fingerprint) |
| Cân bằng nhãn | Chỉ **5/11 nhãn** có video: Abuse 49, Arrest 25, Explosion 24, Arson 22, Fighting 16 | 6 nhãn còn lại **TRẮNG** — xem mục 1.1, đây là vấn đề lớn nhất |
| DAG `social_crawler_youtube` | **đã UNPAUSE** (2026-07-31), **chưa từng có run nào** (`task_instance` rỗng) | 136 video kia đến từ DAG CŨ `social_cctv_video_crawler`, không phải DAG này |
| Cookie YouTube | **KHÔNG có file** `cookies/youtube_cookies.txt` | Discovery vẫn chạy tốt (không cần cookie), nhưng mất video bị age-gate — mục 1.2 |
| Scheduler | **KHÔNG có tiến trình airflow nào đang chạy** | DAG có unpause cũng không tự chạy — phải `./start_airflow.sh` |
| Pool `social_crawler_gpu` | 1 slot, **đã dọn zombie `crawl_x`** (2026-07-31) → slot TRỐNG. Nay có **6** task dùng pool này, gồm cả `find_and_snapshot_streams` | Cách nhận biết + dọn lại nếu tái diễn: mục 1.6 |

**Việc phải làm trước khi bật DAG, theo thứ tự:**
1. ~~Dọn task zombie đang giữ slot GPU~~ — ĐÃ XONG 2026-07-31 (mục 1.6 giữ lại
   cách nhận biết + dọn nếu tái diễn).
2. Quyết cách xử lý mất cân bằng nhãn (mục 1.1) — bật lên như hiện tại thì
   nhãn 6-11 vẫn tiếp tục trắng.
3. Khởi động scheduler: `./start_airflow.sh`.

---

## 1. Quirks đặc thù của YouTube (đã kiểm chứng thực tế)

### 1.1 — Chỉ 5/11 nhãn có dữ liệu: task KHÔNG BAO GIỜ chạy tới nhãn sau
Đây là vấn đề nghiêm trọng nhất, và nó âm thầm làm lệch cả dataset.

Nhãn được xử lý TUẦN TỰ theo đúng thứ tự trong `labels.enabled_labels()`:
```
Abuse, Arrest, Arson, Explosion, Fighting, RoadAccident, Vandalism,
FallOver, Robbery, Shooting, Shoplifting
     └──────── 5 nhãn ĐẦU có data ────────┘  └──── 6 nhãn sau: 0 video ────┘
```
Trùng khớp chính xác với DB. Bằng chứng từ log 2 lần chạy thật:

| Run | Thời lượng | Nhãn chạm tới | Kết thúc |
|---|---|---|---|
| 2026-07-29 | 06:11 → 08:44 (**2h33m**) | Abuse, Arrest, Arson, Explosion, Fighting | `Process terminated by signal` |
| 2026-07-30 | 02:00 → 02:29 (29 phút) | Abuse, Arrest | `Process terminated by signal` |

Không lần nào có dòng tổng kết `XONG [youtube]` ⇒ **chưa lần nào chạy hết**.

**Tính toán cho thấy đây là vấn đề CẤU TRÚC, không phải sự cố ngẫu nhiên:**
```
2h33m / 5 nhãn ≈ 31 phút/nhãn  →  11 nhãn ≈ 5.6 GIỜ
execution_timeout hiện tại      =  3 GIỜ
```
⇒ Kể cả không bị kill, task **không thể** chạy hết 11 nhãn trong 3h. Nhãn
`RoadAccident` trở đi sẽ mãi mãi không có dữ liệu.

Nút thắt KHÔNG phải discovery (mục 1.3) mà là VLM + download.

✅ **ĐÃ SỬA (2026-07-31) — con trỏ nhãn xoay vòng.** Gốc của vấn đề không phải
"3h là quá ít" mà là **thứ tự nhãn cố định**: `labels.LABELS` là dict có thứ tự
và mọi scraper đều lặp từ đầu, nên MỌI lượt chạy đều bắt đầu lại từ `Abuse` và
chết ở cùng một chỗ — chạy 1 lần hay 100 lần đều để `RoadAccident` trở đi trắng.

`platform_crawlers/label_cursor.py` lưu "nhãn kế tiếp" cho từng platform vào
`state/label_cursor.json`; `BaseScraper.iter_label_keywords()` xoay danh sách về
đúng đó. Đã mô phỏng với ngân sách khắc nghiệt (5 lượt discovery/lần chạy):
**phủ đủ 11/11 nhãn sau 10 lượt**, trước đó là 2 nhãn mãi mãi. Đã test thật với
`YouTubeScraper`: lượt 1 xong `Abuse` + 1 từ khóa `Arrest` → con trỏ `Arrest`;
lượt 2 tiếp tục đúng `Arrest` rồi sang `Arson`.

Không cần làm gì thêm. Ba cách cũ giờ chỉ là lựa chọn phụ:

| Cách | Lệnh | Khi nào cần |
|---|---|---|
| Ưu tiên gấp vài nhãn đang trắng | `CRAWL_LABELS="RoadAccident\|Vandalism\|FallOver"` | Muốn lấp nhanh 1 nhóm nhãn, không chờ vòng xoay |
| Xoay vòng nhanh hơn | giảm `CRAWL_TASK_TIME_BUDGET_MINUTES` | Mỗi nhãn ít video hơn nhưng quay lại sớm hơn |
| Tắt xoay vòng | `CRAWL_LABEL_ROTATION=false` | Chỉ khi cần tái hiện đúng một lần chạy cũ |

⚠ **Bẫy của `CRAWL_LABELS`:** `enabled_labels()` kết thúc bằng
`return wanted or list(ALL_LABELS)` — gõ SAI hết tên nhãn thì nó **âm thầm
quay về chạy CẢ 11 NHÃN** thay vì báo lỗi (chỉ có 1 dòng WARNING
`[Labels] Không nhận diện được: [...]` dễ trôi trong log). Đã kiểm chứng:

| `CRAWL_LABELS` | Kết quả |
|---|---|
| `RoadAccident\|Vandalism\|FallOver\|Robbery\|Shooting\|Shoplifting` | 6 nhãn — đúng |
| `RoadAcident\|Vandlism` (gõ sai cả 2) | **11 nhãn** — im lặng chạy hết |
| `road accident` (có dấu cách, thường) | 1 nhãn — vẫn nhận, khớp theo tên hiển thị |

Sau khi đặt biến, LUÔN xác nhận trước khi trigger:
```bash
CRAWL_LABELS="RoadAccident|Vandalism" python -c "
import sys; sys.path.insert(0,'.')
from platform_crawlers import labels as L; print(L.enabled_labels())"
```

### 1.2 — Age-gate chiếm 59% số lỗi tải, và fix được bằng cookie
Đếm trên toàn bộ log `crawl_youtube`:

| Lỗi | Số lần |
|---|---|
| `Sign in to confirm your age` | **13** |
| `Requested format is not available` | 1 |
| (tổng `[FAILED]`) | 22 |

Tái hiện được hôm nay trên đúng video trong log:
```
ERROR: [youtube] jTUDZKZIFlI: Sign in to confirm your age.
       Use --cookies-from-browser or --cookies for the authentication.
```
Hợp lý: nội dung CCTV/crime rất hay bị YouTube gắn hạn chế tuổi.

**Quan trọng — `failed` KHÔNG phải trạng thái terminal**, nên các URL này
được thử lại ở mỗi lần chạy sau (khác `skipped_not_cctv` bị L0 chặn vĩnh
viễn). Tức là chúng đang tốn công VLM + thử tải lại mỗi ngày mà vẫn hỏng.

**Cách khắc phục** — cookie đã có sẵn trong Chrome (đã quét: Default 124
cookie, Profile 1 113 cookie, đều `đã login (SAPISID, __Secure-1PSID)`):
```bash
python scripts/import_browser_cookies.py youtube
# → tạo cookies/youtube_cookies.txt, GenericDownloader tự dùng qua cookies_file_for('youtube')
```
⚠ **Cân nhắc trước khi làm:** yt-dlp cảnh báo dùng cookie YouTube của tài
khoản thật có thể khiến Google khóa/thử thách tài khoản, nhất là khi tải
nhiều. Nếu tài khoản đó quan trọng thì nên dùng tài khoản phụ. Đây là quyết
định của người vận hành, không nên tự ý làm thay.

### 1.3 — Discovery RẺ và NHANH: nút thắt nằm ở VLM + download
Khác hẳn Reddit (198 lượt Playwright, ~54s/lượt) và X (Playwright + throttle):
```
ytsearch40:cctv robbery footage  →  40 URL trong 1 GIÂY
```
YouTube có search extractor sẵn trong yt-dlp, `extract_flat='in_playlist'`
nên chỉ lấy danh sách chứ không resolve từng video. 33 lượt search ≈ **dưới
1 phút** tổng cộng.

⇒ Đừng tối ưu discovery cho YouTube. Muốn nhanh hơn thì phải giảm số URL đi
qua VLM/download (mục 1.1), hoặc tắt VLM (`SOCIAL_USE_CLASSIFIER=false`) khi
chỉ cần thu URL.

Live stream bị loại NGAY ở bước liệt kê (`is_live` / `live_status`), không
tốn chi phí nào — đó là việc của DAG kia (mục 1.4).

### 1.4 — Có HAI DAG YouTube, rất dễ nhầm
| | `social_crawler_youtube` | `camera_source_youtube_live` |
|---|---|---|
| File | `dags/social_crawler_youtube_dag.py` | `dags/camera_source_youtube_live_dag.py` |
| Làm gì | Tải **video có sẵn** (VOD) qua pipeline dedup 4 tầng | Chụp **frame từ luồng LIVE**, chỉ lưu link + ảnh vào file txt |
| Lịch | mỗi 10 phút, xoay vòng qua pool (mục 5) | **18:00, chạy LIÊN TỤC 12 tiếng** tới ~06:00 |
| Trạng thái | **ĐANG BẬT** (unpause 2026-07-31) | **ĐANG BẬT** (`is_paused=0`) |
| Từ khóa | `platform_crawlers/config.py` | `youtube_live/config.py` → `SEARCH_KEYWORDS` |
| Ghi vào | DB `tracker.db` + `data/videos/<nhãn>/` | file txt + ảnh snapshot |
| Pool | `social_crawler_gpu` | `social_crawler_gpu` (đã sửa 2026-07-31, xem mục 1.5) |

Thấy log/kết quả "youtube" mà không khớp mong đợi ⇒ kiểm tra xem đang nói về
DAG nào TRƯỚC KHI debug. Task của DAG kia tên `find_and_snapshot_streams` và
`report_stats`, không phải `crawl_youtube`.

### 1.5 — DAG live KHÔNG dùng GPU pool → có thể OOM chéo với DAG này
`social_crawler_gpu` (1 slot) được tạo ra để đảm bảo tối đa 1 platform nạp
Qwen2.5-VL cùng lúc. Nhưng `find_and_snapshot_streams` chạy ở
**`default_pool`** (đã xác nhận trong `task_instance`) trong khi nó CŨNG dùng
`CCTVClassifier` để xác minh frame.

Cửa sổ chồng lấn: DAG live chạy 18:00→06:00, `social_crawler_youtube` chạy
02:00 ⇒ **trùng nhau 02:00-06:00 mỗi đêm**.

Điều này khớp đáng ngờ với dòng kết thúc của cả 2 run YouTube:
`Process terminated by signal` — dấu hiệu điển hình của bị kill từ bên ngoài
(OOM-killer hoặc scheduler). **Chưa chứng minh được** vì `dmesg`/`journalctl`
trên máy này không đọc được log kernel để tìm dấu vết OOM. Nếu về sau DAG
YouTube lại chết kiểu này, kiểm tra ngay:
```bash
nvidia-smi                       # ai đang giữ VRAM
dmesg -T | grep -i "oom\|killed process"     # cần quyền root
```
**ĐÃ SỬA (2026-07-31):** `find_and_snapshot_streams` giờ dùng
`pool='social_crawler_gpu'` (chỉ khi `YOUTUBE_CCTV_USE_CLASSIFIER=true`, vì tắt
classifier thì task không đụng GPU). Nguy cơ 2 tiến trình cùng nạp Qwen2.5-VL
đã hết, NHƯNG sinh ra hệ quả mới: task này giữ slot **liên tục 12 tiếng**
(18h→6h) nên trong khoảng đó `crawl_youtube` và 4 platform còn lại kẹt `queued`,
không xoay vòng được. Ưu tiên crawl VOD liên tục thì pause DAG live:
```bash
airflow dags pause camera_source_youtube_live
```

### 1.6 — Task ZOMBIE giữ slot GPU → task mới kẹt `queued` mãi mãi
**Đã xảy ra và đã dọn 2026-07-31** (`social_crawler_x / crawl_x`, state=`running`
từ 04:14 mà không có tiến trình airflow nào). Giữ mục này lại vì `pkill -f
airflow` ở đầu `start_airflow.sh` sẽ tạo zombie mới mỗi lần khởi động lại giữa
lúc task đang chạy — đây là nguyên nhân gốc, không phải sự cố một lần.

Airflow tính slot theo `EXECUTION_STATES = {running, queued}`
(`airflow/ti_deps/dependencies_states.py`) ⇒ 1 task mồ côi ở `running` là chặn
đứng cả 6 task dùng pool. Lưu ý `restarting` KHÔNG nằm trong tập đó: `airflow
tasks clear` chuyển zombie sang `restarting` là đã nhả slot, nhưng dag_run vẫn
`running` nên `max_active_runs=1` vẫn chặn DAG đó tạo run mới → phải set cả TI
và dag_run về `failed` (xem lệnh dưới).

Toàn hệ thống còn **11** task kẹt `state=running`, đa số là
`video_processing_4_step` từ tháng 1 (dùng `default_pool` nên vô hại).

**Quy trình dọn (đã dùng thật, thứ tự này quan trọng):**
```bash
export AIRFLOW_HOME="$(pwd)"   # chạy từ thư mục gốc dự án.
# 1. Xem ai đang giữ slot — nhớ liệt kê cả 'restarting'
python -c "
import sqlite3
c = sqlite3.connect('airflow.db')
for r in c.execute(\"SELECT dag_id,task_id,run_id,state,start_date FROM task_instance \"
                   \"WHERE pool='social_crawler_gpu' \"
                   \"AND state IN ('running','queued','restarting')\"):
    print(r)"

# 2. Đối chiếu: có tiến trình thật không? Không có ⇒ zombie.
pgrep -af "airflow" | grep -v "grep"

# 3. clear qua CLI (đưa TI về restarting/None → NHẢ SLOT).
#    ⚠ -e phải là NGÀY SAU logical_date, không phải cùng ngày.
airflow tasks clear social_crawler_x -t crawl_x -y -s 2026-07-31 -e 2026-08-01

# 4. clear KHÔNG đóng dag_run. Còn dag_run 'running' thì max_active_runs=1 vẫn
#    chặn DAG đó tạo run mới ⇒ đóng luôn (backup DB trước khi sửa tay).
cp airflow.db /tmp/airflow.db.bak
python -c "
import sqlite3
c = sqlite3.connect('airflow.db'); cur = c.cursor()
cur.execute(\"UPDATE task_instance SET state='failed' WHERE dag_id='social_crawler_x' \"
            \"AND task_id='crawl_x' AND state='restarting'\"); print(cur.rowcount)
cur.execute(\"UPDATE dag_run SET state='failed', end_date=start_date \"
            \"WHERE dag_id='social_crawler_x' AND state='running'\"); print(cur.rowcount)
c.commit()"
```
⚠ `airflow pools list` **không hiển thị số slot đang dùng** — nó chỉ in
tên/slots/description. Muốn xác nhận slot đã nhả thì query `task_instance` như
bước 1, đừng tin đầu ra của `pools list`.

### 1.7 — Không cần cookie để DISCOVERY, chỉ cần khi TẢI
`YouTubeScraper` gọi `self.cookies()` → `cookies_file_for('youtube')` → hiện
trả `None` vì file không tồn tại. `ytdlp_discover()` chỉ set `cookiefile` khi
file có thật, nên discovery vẫn chạy bình thường (đã đo: 40 URL/1s).

⇒ Khác X và Instagram (`needs_cookies=True`, không có cookie là skip cả
task). YouTube `needs_cookies=False` nên **không bao giờ báo "chưa chạy
được"** — thiếu cookie chỉ làm mất các video age-gate (mục 1.2), âm thầm.

### 1.8 — Từ khóa: YouTube CÓ env override (khác Reddit), ngôn ngữ ngẫu nhiên
```python
by_label = keywords.phrases_by_label(cfg.YOUTUBE_KEYWORDS_OVERRIDE)
```
Có `CRAWL_YOUTUBE_KEYWORDS` — khác Reddit (truyền cứng `None`, không override
được). Nhưng nhớ bẫy chung: đặt override sẽ **mất gán nhãn** (mọi video vào
thư mục mặc định thay vì thư mục theo nhãn), vì `phrases_by_label()` với
override trả `{None: [...]}`.

Mỗi lần chạy Gemini sinh keyword bằng 1 ngôn ngữ NGẪU NHIÊN trong 21 ngôn
ngữ. Bằng chứng trong DB — `source_page` của các URL failed:
```
youtube:Abuse:घरेलू हिंसा सीसीटीवी          (tiếng Hindi)
youtube:Arrest:पुलिस गिरफ्तारी सीसीटीवी
```
Nên số lượng/chất lượng kết quả dao động mạnh giữa các ngày. Muốn cố định:
```bash
export CRAWL_LLM_KEYWORDS=false          # dùng list tĩnh trong labels.py
export CRAWL_KEYWORDS_PER_LABEL=1        # giảm 33 → 11 lượt search
```

### 1.9 — Dedup, portrait, thời lượng
Canonical ID (L1) nhận 5 dạng URL, nên cùng 1 video qua `?v=`, `youtu.be/`,
`/embed/`, `/live/`, `/shorts/` đều dedup đúng. Host nhận diện gồm cả
`m.youtube.com`, `music.youtube.com`, `youtube-nocookie.com`.

- **Shorts** được nhận là `short` — và Shorts gần như luôn 9:16, mà
  `ALLOW_PORTRAIT` rỗng ⇒ bị loại SAU KHI TẢI. Log chỉ có 2 dòng
  `SKIP - 9:16` nên hiện chưa phải vấn đề lớn (khác X: 50%).
- `max_duration_for('youtube')` = **1800s (30 phút)**, mặc định chung.
  YouTube nhiều video dài (compilation, phóng sự) nên ngưỡng này chặn khá
  nhiều — thấy log `dài Ns > giới hạn 1800s` là ĐÚNG thiết kế.

---

## 2. Checklist khi `downloaded` thấp / task không chạy

1. **Task có chạy được không, hay kẹt `queued`?** → mục 1.6 (slot GPU bị
   zombie giữ). Đây là lỗi hay gặp nhất hiện nay và KHÔNG hiện thành lỗi đỏ.
   ```bash
   airflow pools list
   ```
2. **Scheduler có sống không?**
   ```bash
   ps -eo pid,etime,cmd | grep -E 'airflow (scheduler|dag-processor)' | grep -v grep
   ```
   Không có gì ⇒ chạy `./start_airflow.sh`. (2026-07-31: đang KHÔNG chạy.)
3. **Đang xem đúng DAG chưa?** → mục 1.4. `camera_source_youtube_live` là DAG
   khác hẳn và nó mới là cái đang bật.
4. **Task chết giữa chừng?** Tìm dòng cuối log:
   ```bash
   L="logs/dag_id=social_crawler_youtube/run_id=<run_id>/task_id=crawl_youtube/attempt=1.log"
   python - "$L" <<'PY'
   import json, sys
   recs = []
   for l in open(sys.argv[1]):
       try: recs.append(json.loads(l))
       except Exception: pass
   print('từ', recs[0].get('timestamp','')[:19], 'đến', recs[-1].get('timestamp','')[:19])
   for r in recs[-3:]: print(' ', str(r.get('event'))[:150])
   PY
   ```
   - `Process terminated by signal` → bị kill từ ngoài (nghi OOM, mục 1.5)
   - `Server indicated the task shouldn't be running` → bị clear/mark-failed trên UI
   - Không có dòng `XONG [youtube]` → chưa chạy hết, nhãn sau chưa được đụng tới
5. **Nhãn nào đã có dữ liệu?** → query ở mục 3. Thiếu 6 nhãn cuối là triệu
   chứng của mục 1.1, KHÔNG phải YouTube hết video cho nhãn đó.
6. **Nhiều `failed`?** → đếm age-gate:
   ```bash
   # dùng find, KHÔNG dùng logs/**/... — bash mặc định TẮT globstar nên
   # glob ** không đệ quy và lệnh sẽ âm thầm trả 0 (đã bị lừa 1 lần)
   find logs -path "*task_id=crawl_youtube*" -name "*.log" \
        -exec grep -ho "Sign in to confirm your age" {} + | wc -l
   ```
   Ra số lớn ⇒ mục 1.2, nhập cookie YouTube (cân nhắc rủi ro tài khoản).

---

## 3. Truy vấn DB nhanh

DB: `data/db/tracker.db`. Lưu ý tên cột thật (dễ nhầm):
`video_urls` có `final_category` và `category_hint`, **không** có `category`;
`sources` có `source_type`/`category`, **không** có `source_name`.

```bash
python -c "
import sqlite3
c = sqlite3.connect('data/db/tracker.db')
print('sources     :', c.execute('SELECT COUNT(*) FROM sources WHERE source_type=\"youtube\"').fetchone()[0])
print('video_urls theo status:')
for r in c.execute('SELECT status, COUNT(*) FROM video_urls WHERE source_platform=\"youtube\" GROUP BY status ORDER BY 2 DESC'):
    print('   ', r)
print('fingerprints:', c.execute('SELECT COUNT(*) FROM video_fingerprints WHERE source_platform=\"youtube\"').fetchone()[0])
print()
print('ĐÃ TẢI theo nhãn (kiểm tra cân bằng dataset):')
for r in c.execute('SELECT final_category, COUNT(*) FROM video_urls '
                   'WHERE source_platform=\"youtube\" AND status=\"downloaded\" '
                   'GROUP BY final_category ORDER BY 2 DESC'):
    print('   ', r)
"
```
Mốc so sánh 2026-07-31: 136 downloaded / 50 skipped_not_cctv / 22 failed /
3 pending, và chỉ 5 nhãn có mặt.

Xem URL nào đang `failed` để biết lý do:
```bash
python -c "
import sqlite3
c = sqlite3.connect('data/db/tracker.db')
for r in c.execute('SELECT url, source_page FROM video_urls '
                   'WHERE source_platform=\"youtube\" AND status=\"failed\" LIMIT 10'):
    print(r[0][:55], '|', r[1])"
```

---

## 4. Vận hành Airflow

```bash
export AIRFLOW_HOME="$(pwd)"   # chạy từ thư mục gốc dự án — KHÔNG có hậu tố /airflow
```
Máy này có 3 giá trị AIRFLOW_HOME dễ nhầm; nếu scheduler đang chạy thì xác
minh bằng process thật:
```bash
tr '\0' '\n' < /proc/<PID>/environ | grep AIRFLOW_HOME
```

```bash
./start_airflow.sh                              # hiện KHÔNG có tiến trình nào chạy
airflow dags unpause social_crawler_youtube     # đang PAUSE
airflow dags list-runs social_crawler_youtube -o plain    # dag_id POSITIONAL
airflow tasks clear social_crawler_youtube -t crawl_youtube -s <date> -e <date> -d -y

# Chạy thử nhanh, không đợi hết cả vòng:
airflow dags trigger social_crawler_youtube \
  --conf '{"max_per_label": 3, "max_per_platform": 15, "time_budget_minutes": 15}'
```
`time_budget_minutes` cho task tự dừng ĐÚNG LỊCH giữa các batch thay vì bị
SIGKILL giữa lúc đang tải (kill cứng để lại file dở + ffmpeg mồ côi).

Log: `logs/dag_id=social_crawler_youtube/run_id=<run_id>/task_id=crawl_youtube/attempt=N.log`
(JSON mỗi dòng 1 record — `json.loads` rồi đọc field `event`).
Log của các lần chạy CŨ nằm dưới `dag_id=social_cctv_video_crawler/.../task_id=crawl_youtube/`.

---

## 5. Pool GPU dùng chung

`social_crawler_gpu` (1 slot) — chia sẻ với 4 DAG platform khác **và** DAG live
(6 task tổng cộng). Đây chính là cơ chế "mỗi lúc chỉ 1 crawler": 5 DAG platform
đều đặt `schedule=ROTATION_SCHEDULE` (10 phút) + `max_active_runs=1`, nên luôn có
run xếp hàng, pool cho đúng 1 cái đi qua, xong là cái tiếp theo vào → xoay vòng
liên tục. Chi tiết ở docstring `dags/social_crawler_common.py`.

Hai lưu ý riêng cho YouTube:
- Task YouTube chạy LÂU nhất (2h33m mà mới xong 5/11 nhãn) nên **nó sẽ độc chiếm
  vòng xoay** nếu không đặt trần thời gian mỗi lượt. `start_airflow.sh` đang
  export `CRAWL_TASK_TIME_BUDGET_MINUTES=60`; bỏ biến đó là YouTube ăn hết slot.
- DAG `camera_source_youtube_live` giờ ĐÃ nằm trong pool này (mục 1.5), nhưng nó
  giữ slot liên tục 12h nên nửa ngày sẽ không có platform nào chạy được.

---

## 6. Nếu định sửa code — xếp theo giá trị/công sức

1. ~~Xử lý mất cân bằng nhãn~~ — ĐÃ LÀM 2026-07-31 bằng con trỏ nhãn xoay vòng
   (`platform_crawlers/label_cursor.py`, mục 1.1). Việc còn lại chỉ là theo dõi:
   `python scripts/crawl_report_24h.py` in bảng 11 nhãn + con trỏ hiện tại; nếu
   danh sách nhãn trắng không ngắn lại sau vài ngày thì mới cần can thiệp.
2. ~~Cho `find_and_snapshot_streams` dùng `pool='social_crawler_gpu'`~~ — ĐÃ
   LÀM 2026-07-31 (mục 1.5). Việc còn lại là quyết định pause DAG live hay
   không, vì nó giữ slot 12h/ngày.
3. **Nhập cookie YouTube** (mục 1.2) — thu hồi 13 video age-gate, nhưng cân
   nhắc rủi ro tài khoản Google. Không phải sửa code.
4. **Dọn task zombie định kỳ** (mục 1.6) — hoặc thêm bước kiểm tra trong
   `start_airflow.sh`: task `state=running` mà không có tiến trình thì reset.

---

## 7. Trước khi kết luận "đây là bug mới"

1. **Xác định đúng DAG** (mục 1.4) — hai DAG YouTube làm việc hoàn toàn khác
   nhau, và cái đang BẬT lại là cái kia.
2. **Kiểm tra slot pool và scheduler trước khi debug logic** (mục 1.6, 1.2 ở
   checklist) — "DAG không chạy" ở đây thường là hạ tầng, không phải code.
3. Query DB trực tiếp xem nhãn nào có dữ liệu — đừng suy từ log tóm tắt.
4. Tái hiện bằng script Python độc lập, KHÔNG qua Airflow. Với YouTube rất
   rẻ: `ytdlp_discover(youtube_search_target(kw, 40), ...)` chạy 1 giây.
5. **Đừng mang kinh nghiệm platform khác sang**: YouTube KHÔNG cần cookie để
   search (khác X/Instagram), discovery KHÔNG phải nút thắt (khác Reddit),
   và KHÔNG có chuyện bị throttle search (khác X). Nút thắt duy nhất là
   VLM + download.
