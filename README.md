# Computer Vision Data Collection

Dự án dùng Apache Airflow 3 để thu thập dữ liệu huấn luyện từ hai loại nguồn:

1. **Dataset video sự việc** (phần chính). Tự động tìm, tải, lọc và gán nhãn video CCTV
   ghi lại sự việc bất thường: cướp, đánh nhau, tai nạn, cháy nổ… Nguồn là YouTube,
   Dailymotion, Reddit, X và Facebook. Kết quả chia theo 27 nhãn (mở rộng từ UCF-Crime).
2. **Dataset ảnh từ camera đang phát.** Lấy frame từ camera RTSP và luồng YouTube live,
   nhận diện bằng YOLO, rồi lưu ảnh có nhãn cho ba bài toán: biển số xe (ANPR), thuộc
   tính người và mũ bảo hiểm.

- [Kiến trúc](#kiến-trúc)
- [Các DAG](#các-dag)
- [Nhóm DAG camera](#nhóm-dag-camera)
- [Cài đặt trên máy mới](#cài-đặt-trên-máy-mới)
- [Vận hành hằng ngày](#vận-hành-hằng-ngày)
- [Dữ liệu đầu ra](#dữ-liệu-đầu-ra)
- [Phát triển tính năng](#phát-triển-tính-năng)
- [Xử lý sự cố](#xử-lý-sự-cố)

---

## Kiến trúc

### Luồng một video

Mọi URL từ mọi platform đều **bắt buộc** đi qua một cổng chung,
`crawler_core.pipeline.VideoPipeline`. Scraper chỉ có nhiệm vụ tìm URL; nó không tự tải
và không tự lọc.

```
Scraper (theo platform) ── URL ──►  L0  URL đã xử lý?           ┐ chưa tải
                                    L1  trùng ID gốc platform?   ┘ (chắc chắn trùng)
                                    ⬇  TẢI VỀ ổ nội bộ (staging)
                                    9:16  video dọc?             ┐
                                    L2  trùng SHA-256?           │ bị loại → video_rejected/<lý do>/<nhãn>/
                                    L3  trùng phash frame giữa?  │   kèm <file>.json ghi lý do
                                    L4  trùng fingerprint 5 frame│   (hoặc xóa: CRAWL_REJECT_ACTION=delete)
                                    CLIP  có phải CCTV?          │
                                    VideoMAE  có sự việc?        ┘
                                    ✓  chuyển vào <dataset>/<nhãn>/ + lưu fingerprint
```

- Bộ lọc được sắp **rẻ trước, đắt sau**, và dừng ngay khi video bị loại.
- **Lý do loại luôn được ghi vào DB**, ở `video_urls.reject_reason` và `filter_detail`.
  Nhờ vậy anh/chị đánh giá được từng bộ lọc có loại đúng không.
- **Mỗi bộ lọc bật/tắt được theo từng platform** ngay trên giao diện web.

Các DAG dùng GPU chung **pool `social_crawler_gpu` 1 slot**, nên mỗi lúc chỉ một task
nạp model lên GPU. Nếu không có pool này, máy 6 GB sẽ hết VRAM.

### Cấu trúc thư mục

```
dags/                   Định nghĩa DAG — MỎNG, logic nằm ở các package bên dưới
  social_crawler_common.py   factory DAG + task crawl + bộ lọc bật/tắt + vòng xoay
  social_crawler_<platform>_dag.py   1 file / platform (dataset video sự việc)
  camera_dataset_dags.py     3 DAG camera_dataset_* (ANPR, thuộc tính người, mũ BH)
  camera_source_youtube_live_dag.py  tìm luồng YouTube live làm nguồn camera
  legacy_fb_groups_crawler_dag.py    DAG cũ quét nhóm Facebook
crawler_core/           Lõi dùng chung mọi platform
  pipeline.py                VideoPipeline — cổng lọc bắt buộc
  dedup.py                   canonical ID (L1), hash/phash/fingerprint (L2–L4)
  downloader.py              yt-dlp → staging → dataset/NAS, đường dẫn lưu
  db_manager.py              SQLite tracker (video_urls, video_fingerprints, sources)
  cctv_detector.py           bộ lọc CCTV: CLIP + OCR timestamp + fps thực (+ CLI scan/train)
  qwen_classifier.py         bộ lọc CCTV cũ bằng Qwen2.5-VL (CRAWL_CCTV_CLASSIFIER=qwen, DAG YouTube live)
  videomae_filter.py         bộ lọc sự việc VideoMAE
  facebook_downloader.py     tải video Facebook (bắt DASH bằng Playwright)
  gemini_client.py           gọi Gemini xoay vòng key/model (sinh từ khóa)
  third_party/videomae/      code kiến trúc VideoMAE (vendored, CC BY-NC 4.0)
platform_crawlers/      Tìm URL theo platform
  scrapers.py                1 class / platform + registry
  discovery.py               yt-dlp / Playwright / API từng platform
  labels.py                  27 nhãn sự việc + mô tả (đưa vào prompt Gemini)
  keywords.py                sinh từ khóa đa ngôn ngữ bằng Gemini
  config.py                  mọi tham số CRAWL_* (override bằng env)
  facebook_groups/           scraper nhóm Facebook cho DAG legacy_fb_groups_crawler
youtube_live/           logic của camera_source_youtube_live: search, lọc tiêu đề, chụp, xác minh
src/                    logic của camera_dataset_*: đọc camera, YOLO, PaddleOCR, PaddleClas
  modules/                   config (model, RTSP_FILE, OUTPUT_DIR), đọc luồng, che mật khẩu
  pipeline/tasks/            từng bước: capture → detect → crop → OCR / thuộc tính / mũ BH → lưu
rtsp/                   danh sách camera (không có trong git, trừ cameras.example.txt)
scripts/                setup.sh, doctor.py, cookie, báo cáo, gộp nhãn, backfill
config/                 gemini_keys.json (không có trong git; mẫu: gemini_keys.example.json)
data/                   (không có trong git) db/tracker.db = lịch sử dedup, staging, dataset nội bộ
cookies/                (không có trong git) cookie đăng nhập từng platform
models/                 (không có trong git) xem models/README.md
.claude/skills/         Hướng dẫn vận hành / phát triển cho Claude Code và người đọc
```

---

## Các DAG

Tên DAG có tiền tố theo nhóm, nhìn tiền tố là biết DAG thuộc phần nào:

| Tiền tố | Nhóm | Đầu ra |
|---|---|---|
| `social_crawler_*` | Dataset video sự việc | Video `.mp4` theo 27 nhãn trên NAS |
| `camera_dataset_*` | Dataset ảnh từ camera đang phát | Ảnh + nhãn YOLO trong `data/` |
| `camera_source_*` | Tìm nguồn camera mới | Danh sách link live |
| `legacy_*` | Bản cũ, giữ lại khi cần | |

| DAG | File | Làm gì | Lịch | GPU pool |
|---|---|---|---|---|
| `social_crawler_youtube` · `_dailymotion` · `_reddit` · `_x` · `_fb` | `social_crawler_<platform>_dag.py` | Tìm video theo từ khóa của 27 nhãn → `VideoPipeline` (tải, lọc, lưu) | Nối vòng theo Variable `crawler_rotation` | ✓ |
| `camera_dataset_vehicle_anpr` | `camera_dataset_dags.py` | Frame camera → YOLO xe → PaddleOCR biển số | Chạy tay | ✗ |
| `camera_dataset_person_attributes` | `camera_dataset_dags.py` | Frame camera → YOLO người → PaddleClas thuộc tính | Chạy tay | ✗ |
| `camera_dataset_helmet` | `camera_dataset_dags.py` | Frame camera → YOLO xe → YOLO mũ bảo hiểm | 7h mỗi ngày, chạy 10 tiếng | ✗ |
| `camera_source_youtube_live` | `camera_source_youtube_live_dag.py` | Tìm luồng CCTV đang live trên YouTube, chụp frame, xác minh bằng Qwen2.5-VL | 18h mỗi ngày, chạy 12 tiếng | ✓ (giữ slot 12 tiếng) |
| `legacy_fb_groups_crawler` | `legacy_fb_groups_crawler_dag.py` | Tải video trong các nhóm Facebook đã tham gia → `VideoPipeline`, nhãn chung `CCTV` | 24 giờ/lần | ✗ |

- **Dễ nhầm:** `social_crawler_youtube` *tải video* sự việc. `camera_source_youtube_live`
  *chỉ tìm link* camera đang phát trực tiếp.
- **Dễ nhầm:** `social_crawler_fb` *search* theo nhãn. `legacy_fb_groups_crawler` quét
  *nhóm* đã tham gia. Hai DAG dùng chung DB dedup nên không tải trùng.
- `legacy_fb_groups_crawler` và `camera_source_youtube_live` hiện đang **paused**.
- **Đổi tên ngày 2026-09-28.** Tên cũ là `get_data_vehicle_anpr`,
  `get_data_person_attributes`, `get_data_helmet_attributes`, `youtube_cctv_live_crawler`
  và `fb_cctv_video_crawler`. Lịch sử chạy cũ vẫn nằm trong `airflow.db` dưới tên cũ.

---

## Nhóm DAG camera

```
camera_source_youtube_live          rtsp/*.txt  (chép tay)            camera_dataset_*
  YouTube search "live cctv…"  ─►  live_streams.txt  ─ ─ ─ ─ ─►  mỗi camera: đọc frame
  lọc tiêu đề, bỏ luồng trùng       + snapshots/                     → YOLO → crop
  chụp frame → Qwen2.5-VL          (1.196 luồng, 2026-07)           → OCR / thuộc tính / mũ BH
                                                                     → data/<loại>/
```

**Danh sách camera.** Mỗi DAG `camera_dataset_*` đọc một file, được khai báo ở `RTSP_FILE`
trong `src/modules/config.py`:

| DAG | File camera | Đầu ra |
|---|---|---|
| `camera_dataset_vehicle_anpr` | `rtsp/cam_vh_lp.txt` | `data/vehicle/` (frame + nhãn YOLO), `data/anpr/` (crop biển số + chữ OCR) |
| `camera_dataset_person_attributes` | `rtsp/cam_person.txt` | `data/person/`, `data/attributes/` |
| `camera_dataset_helmet` | `rtsp/cam_vh_helmet.txt` | `data/helmet/`: chỉ lưu crop xe có mũ thường, hoặc có từ 2 lớp trở lên (mũ BH, mũ trùm, không mũ, điện thoại) |

- Mỗi dòng trong file là một URL. Có hai dạng:
  - RTSP: `rtsp://{rtsp_cam_1}@host:554/...`. `{rtsp_cam_1}` là Airflow Connection giữ
    tài khoản camera, xem [Cài đặt, bước 3](#3-cấu-hình-riêng-của-máy).
  - Link YouTube live. `src/modules/video_capture.py` đổi link này sang luồng phát bằng yt-dlp.
- Các file này **không có trong git**. Mẫu ở `rtsp/cameras.example.txt`.
- **Luồng tìm được chưa tự vào `camera_dataset_*`.** `camera_source_youtube_live` ghi
  kết quả vào `data/videos/youtube_cctv/live_streams.txt`, mỗi dòng gồm link, ảnh và tiêu
  đề. Muốn dùng thì chép link sang file `rtsp/*.txt` tương ứng.

**Chế độ chạy.** Biến `VIDEO_PIPELINE_RUN_MODE` có hai giá trị:
- `parallel` (mặc định): mỗi camera một thread. Lấy được kết quả thì camera đó nghỉ 60 giây.
- `sequential`: lần lượt từng camera. Mỗi camera thử tối đa `max_frames` frame.

**Cài thêm.**
- Thư viện: `bash scripts/setup.sh --rtsp`, tức `requirements-optional.txt` mục (a):
  ultralytics, PaddleOCR, PaddleClas.
- Model YOLO, OCR và thuộc tính: xem [models/README.md](models/README.md).
- Nếu chưa cài, ba DAG này báo lỗi import trên web. Các DAG khác vẫn chạy bình thường.

⚠ **Ba DAG `camera_dataset_*` chạy YOLO trên GPU nhưng chưa khai báo pool
`social_crawler_gpu`.** `camera_dataset_helmet` chạy 7h–17h, trùng giờ với vòng xoay
crawler. Nếu gặp CUDA out of memory, hãy dừng vòng xoay trong giờ đó hoặc pause DAG này.

---

## Cài đặt trên máy mới

**Yêu cầu:**
- Ubuntu 22.04 (đã kiểm thử), Python 3.10.
- GPU NVIDIA với driver hỗ trợ CUDA 11.8. Máy gốc dùng GTX 1660 SUPER 6 GB.
- Khoảng 20 GB trống cho venv, model và cache. Dung lượng dữ liệu video tùy lượng crawl.

Máy không có GPU vẫn chạy được, xem cờ `--cpu` ở bước 2, nhưng phải tắt hoặc chấp nhận
bộ lọc CLIP và VideoMAE chạy rất chậm.

### 1. Gói hệ thống

```bash
sudo apt update
sudo apt install -y python3.10 python3.10-venv ffmpeg tesseract-ocr git
curl -fsSL https://deno.land/install.sh | sh     # yt-dlp cần deno để tải YouTube
```

### 2. Clone và cài thư viện

```bash
git clone https://github.com/tranquoctuan-hnue/Airflow_end_to_end.git
cd Airflow_end_to_end
bash scripts/setup.sh            # thêm --rtsp nếu chạy pipeline camera, --cpu nếu không có GPU
```

`setup.sh` làm lần lượt các việc sau, và chạy lại nhiều lần cũng an toàn:
1. Tạo venv `airflow_venv/`.
2. Cài Airflow 3.1.6 kèm file constraints chính thức.
3. Cài PyTorch CUDA 11.8 và `requirements.txt`.
4. Cài trình duyệt Chromium cho Playwright.
5. Tạo `.env` và `gemini_keys.json` từ file mẫu.
6. Khởi tạo DB Airflow và pool GPU.
7. Cuối cùng chạy `scripts/doctor.py` để báo còn thiếu gì.

### 3. Cấu hình riêng của máy

| Việc | Cách làm |
|---|---|
| **Nơi lưu video** (bắt buộc) | Sửa `CRAWL_VIDEO_OUTPUT_DIR` trong `.env`. Giá trị mặc định là NAS của máy gốc, máy khác sẽ không có |
| **Model VideoMAE** | Chép vào `models/`, xem [models/README.md](models/README.md). Không có model thì đặt `SOCIAL_USE_VIDEOMAE=false` |
| **Gemini API key** | Điền vào `config/gemini_keys.json`. Không có key thì crawler dùng từ khóa tiếng Anh tĩnh |
| **Cookie đăng nhập** | Đăng nhập platform trong Chrome thường, rồi chạy `airflow_venv/bin/python scripts/import_browser_cookies.py x`. **X và Facebook bắt buộc có cookie**; Reddit nên có |
| **Reddit OAuth** (nên có) | `REDDIT_CLIENT_ID` và các biến liên quan trong `.env`, xem `.claude/skills/reddit-dag` |
| **Camera RTSP** (chỉ pipeline camera) | Danh sách URL để trong `rtsp/*.txt` dạng `rtsp://{rtsp_cam_1}@host:554/...` (mẫu `rtsp/cameras.example.txt`). Tài khoản camera tạo ở **Admin → Connections**: id `rtsp_cam_1`, Login và Password. **Không ghi mật khẩu vào file** |
| **Lịch sử dedup** (tùy chọn) | Chép `data/db/tracker.db` từ máy gốc sang, để máy mới không tải lại hàng chục nghìn URL đã xử lý |

⚠ **Không cho hai máy crawl cùng lúc với hai DB khác nhau.** Mỗi máy chỉ biết lịch sử
của riêng mình, nên sẽ tải trùng video của nhau.

### 4. Kiểm tra và chạy

```bash
airflow_venv/bin/python scripts/doctor.py        # ✓ hết thì chạy được
./start_airflow.sh                               # mở http://localhost:8080
```

- **Đăng nhập:** có ba tài khoản `admin`, `op` và `user`, khai báo trong `start_airflow.sh`.
  Mật khẩu nằm trong file `simple_auth_manager_passwords.json.generated`, tự sinh ở lần
  chạy đầu. Quyền của từng tài khoản:
  - `user`: xem, Trigger và pause DAG;
  - `op`: thêm quyền sửa Connections, Variables, Pools và chạy Backfill;
  - `admin`: toàn quyền.
- **Chạy thử an toàn:** mở DAG `social_crawler_youtube` → **Trigger**, tick
  **"Chế độ test"**, đặt "Trần thời gian" = 2. Lượt test ghi mọi thứ vào
  `data/.test_runs/`, không đụng DB thật hay dataset.

---

## Vận hành hằng ngày

Mọi thao tác đều làm được trên giao diện web, không cần sửa code.

| Muốn | Làm |
|---|---|
| **Bắt đầu vòng crawl** | Trigger một DAG `social_crawler_*`, tick **"Tiếp tục vòng xoay"** |
| Đổi thứ tự hoặc danh sách platform | Admin → Variables → `crawler_rotation`: `{"enabled": true, "order": ["youtube","x","reddit"]}` |
| **Dừng vòng** (an toàn) | Sửa `crawler_rotation` thành `"enabled": false`. DAG đang chạy xong lượt của nó rồi dừng |
| Tắt một bộ lọc cho một lượt | Form Trigger: các ô *Lọc CCTV / 9:16 / trùng / VideoMAE* → **Tắt** |
| Tắt một bộ lọc lâu dài | Variable `crawler_filters`: `{"youtube": {"cctv": false}, "*": {"portrait": false}}` |
| Chạy thử không ghi DB | Form Trigger: tick **"Chế độ test"** |
| Chạy N lượt liền | Trigger → **Backfill**, mỗi ngày trong khoảng chọn là một lượt |
| Dừng ngay một task | Grid → bấm vào task → **Mark as → Failed** |

- **Khóa của `crawler_filters`:** `cctv`, `portrait`, `dedup`, `videomae`.
  Khóa platform `"*"` nghĩa là áp dụng cho mọi platform.
- **Thứ tự ưu tiên khi quyết định bật/tắt bộ lọc:** form Trigger > Variable > mặc định
  trong `.env` và `start_airflow.sh`.
- **Log đầu mỗi lượt chạy** ghi rõ từng bộ lọc đang bật hay tắt, và lấy từ nguồn nào.

**Truy cập từ máy khác trong mạng LAN:** web đã nghe trên `0.0.0.0:8080`. Mở cổng trên
firewall bằng `sudo ufw allow from <dải LAN> to any port 8080`, rồi vào
`http://<IP máy chạy>:8080`. **Không mở cổng 8080 ra internet**: web chỉ có HTTP và trình
đăng nhập đơn giản. Muốn truy cập từ xa thì dùng VPN, ví dụ Tailscale.

---

## Dữ liệu đầu ra

```
<CRAWL_VIDEO_OUTPUT_DIR>/<Nhãn>/<platform>_<id>.mp4          video đạt mọi bộ lọc
<video_rejected>/<lý do>/<Nhãn>/<file>.mp4 + .json + ảnh     video bị loại (để kiểm tra bộ lọc)
data/db/tracker.db                               lịch sử mọi URL + fingerprint
```

Các lý do loại là `portrait`, `dup_l1`…`dup_l4`, `not_cctv`, `uncertain_cctv`,
`no_valid_frame` và `no_event`. Một số câu SQL hữu ích:

```sql
-- Mỗi bộ lọc loại bao nhiêu video
SELECT reject_reason, COUNT(*) FROM video_urls WHERE reject_reason IS NOT NULL GROUP BY 1;
-- Video được nhận khi bộ lọc CCTV đang tắt (chưa từng được kiểm tra CCTV)
SELECT file_path FROM video_urls
WHERE status='downloaded' AND json_extract(filter_detail,'$.filters_enabled.cctv') = 0;
```

Báo cáo 24 giờ: `airflow_venv/bin/python scripts/crawl_report_24h.py`, kết quả ghi vào
`reports/`.

---

## Phát triển tính năng

### Quy tắc bất di bất dịch

1. **Scraper chỉ tìm URL.** Mọi việc tải, lọc và ghi DB phải đi qua `VideoPipeline`.
   Không gọi downloader trực tiếp.
2. **Không viết cứng đường dẫn máy.** Tính từ thư mục dự án (`os.path.dirname(__file__)`),
   và cho phép ghi đè bằng biến môi trường.
3. **Không commit secret.** Key, cookie, `.env`, `airflow.cfg` và danh sách RTSP đều đã
   nằm trong `.gitignore`.
4. **Không ghi secret ra log.** Mật khẩu camera lấy từ Airflow Connection và được che
   thành `***` (`src/modules/rtsp_credentials.py`). Khi in URL RTSP, luôn bọc bằng
   `redact(url)`.
5. **Luôn thử bằng Chế độ test** trước khi cho chạy thật.

### Hướng dẫn chi tiết (skills)

Thư mục `.claude/skills/` chứa hướng dẫn từng bước. Claude Code tự dùng đúng skill theo
việc đang làm; người đọc có thể mở file `SKILL.md` ra xem trực tiếp.

| Skill | Dùng khi |
|---|---|
| `setup-machine` | Cài dự án lên máy mới, hoặc chuyển máy |
| `add-platform` | Thêm nguồn video mới, ví dụ TikTok hoặc Bilibili |
| `add-filter` | Thêm một điều kiện lọc mới vào pipeline |
| `manage-labels` | Thêm, sửa hoặc gộp nhãn sự việc |
| `test-pipeline` | Chạy thử pipeline hoặc vòng xoay mà không đụng dữ liệu thật |
| `youtube-dag`, `x-dag`, `reddit-dag`, `fb-dag`, `dailymotion-dag` | Debug từng platform: cookie, lỗi tải, 0 URL |

### Kiểm thử nhanh không cần mạng

Pipeline nhận downloader và classifier qua tham số. Vì vậy có thể thay downloader bằng
một lớp giả chép file có sẵn và dùng DB tạm, để test bộ lọc offline. Xem skill
`test-pipeline`.

---

## Xử lý sự cố

Đầu tiên luôn chạy `airflow_venv/bin/python scripts/doctor.py`.

| Triệu chứng | Nguyên nhân thường gặp | Sửa |
|---|---|---|
| YouTube: `downloaded=0`, lỗi HTTP 403 | yt-dlp cũ | `airflow_venv/bin/pip install -U yt-dlp` |
| Mọi DAG crawler lỗi ngay khi bắt đầu | Chưa mount NAS / `CRAWL_VIDEO_OUTPUT_DIR` sai | Mount NAS, hoặc sửa đường dẫn trong `.env` |
| X / Facebook ra 0 URL nhưng task vẫn SUCCESS | Cookie hết hạn | Đăng nhập lại trên Chrome → `scripts/import_browser_cookies.py <platform>` |
| Task kẹt ở trạng thái `queued` mãi | Mất pool GPU, hoặc DAG khác đang giữ slot | `airflow pools set social_crawler_gpu 1 "GPU"`; kiểm tra `camera_source_youtube_live` (giữ slot 12 tiếng) |
| CUDA out of memory | Hai model chạy cùng lúc, hoặc có tiến trình khác chiếm GPU | `nvidia-smi` để tìm tiến trình; đảm bảo task nằm trong pool |
| VideoMAE rất chậm | GPU không có tensor core mà lại bật fp16 | Giữ `VIDEOMAE_FP16=false` (mặc định) |
