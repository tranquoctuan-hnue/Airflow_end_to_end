---
name: setup-machine
description: Cài dự án lên máy mới sau khi clone, chuyển sang máy khác, hoặc sửa môi trường bị hỏng — venv, Airflow 3.1.6, PyTorch CUDA, Playwright, deno, .env, model VideoMAE, Gemini key, cookie, DB lịch sử dedup. Dùng khi người dùng hỏi "cài đặt thế nào", "chạy trên máy khác", "clone về không chạy được", doctor.py báo lỗi, hoặc thiếu thư viện/model.
---

# Cài đặt / chuyển máy

Ba file là nguồn sự thật: `scripts/setup.sh` (cài), `scripts/doctor.py` (kiểm tra, chỉ
đọc), `README.md` mục "Cài đặt trên máy mới". Khi thêm thư viện hay bước cài mới, sửa
**cả ba** cho khớp.

## Quy trình

1. **Gói hệ thống.** Cần sudo nên người dùng phải tự chạy:
   ```bash
   sudo apt install -y python3.10 python3.10-venv ffmpeg tesseract-ocr
   curl -fsSL https://deno.land/install.sh | sh
   ```
2. **Cài thư viện:** `bash scripts/setup.sh` (thêm `--rtsp`, `--qwen` hoặc `--cpu` nếu cần).
3. **Kiểm tra:** chạy `airflow_venv/bin/python scripts/doctor.py`, sửa cho tới khi hết ✗.
   Mục ⚠ là tính năng bị thiếu, vẫn chạy được.
4. **Cấu hình máy:** `.env` (mẫu ở `.env.example`). Ít nhất phải đặt
   `CRAWL_VIDEO_OUTPUT_DIR`.
5. **Model:** làm theo `models/README.md`. Chép VideoMAE bản gọn bằng
   `scripts/slim_checkpoint.py`: 3,4 GB → 1,1 GB, kết quả giống hệt.
6. **Chạy** `./start_airflow.sh`, rồi thử bằng Chế độ test (skill `test-pipeline`).

## Bẫy đã gặp thật

| Vấn đề | Chi tiết |
|---|---|
| **yt-dlp cũ → YouTube HTTP 403 toàn bộ** | Bản 2026.07.04 hỏng, 2026.08.19 chạy được (đo 2026-09-28). YouTube đổi cơ chế liên tục, nên khi YouTube `downloaded=0` thì việc đầu tiên là `pip install -U yt-dlp`. Không có deno thì yt-dlp cũng không giải mã được YouTube |
| **Nơi lưu mặc định là NAS của máy gốc** | Đường dẫn GVFS `/run/user/<uid>/gvfs/smb-share:server=pvn_nas.local,...`. Máy khác không có NAS này, nên mọi DAG crawler chết ngay ở `ensure_output_base()` nếu chưa đặt `CRAWL_VIDEO_OUTPUT_DIR`. GVFS còn mất mount khi đăng xuất; chạy như dịch vụ thì phải mount CIFS qua `/etc/fstab` |
| **fp16 chậm hơn fp32 ~5 lần trên GTX 1660** | Card không có tensor core (4,4 so với 0,9 giây/view). Giữ `VIDEOMAE_FP16=false` trừ khi là GPU RTX |
| **Hai máy crawl, hai DB riêng** | Dedup L0–L4 chỉ biết DB của máy mình, nên hai máy tải trùng video của nhau. Chép `data/db/tracker.db` sang máy mới, và không cho hai máy crawl song song |
| **Mất pool GPU** | Pool nằm trong `airflow.db`. `airflow db reset` xóa mất pool, và task khai báo pool không tồn tại sẽ kẹt `queued` mãi. `start_airflow.sh` và `setup.sh` đều tạo lại pool |
| **`airflow.cfg` không có trong git** | Có `secret_key`/`jwt_secret` và đường dẫn tuyệt đối. `airflow db migrate` tự sinh lại theo `AIRFLOW_HOME`. Cấu hình Airflow cần đổi thì đặt bằng biến `AIRFLOW__SECTION__KEY` trong `start_airflow.sh`, không sửa cfg |
| **Mật khẩu admin** | SimpleAuthManager sinh `simple_auth_manager_passwords.json.generated` ở lần chạy đầu. Tài khoản khai báo trong `[core] simple_auth_manager_users` |
| **transformers bản dev** | Máy gốc dùng 5.13.0.dev0 (cài từ git), nhưng chỉ Qwen cần tới. Crawler mặc định (CLIP + VideoMAE) không import transformers |

## Camera RTSP (chỉ khi chạy pipeline camera)

- `rtsp/*.txt` **không có trong git**. Mỗi dòng là một URL dạng
  `rtsp://{rtsp_cam_1}@host:554/...`; `{rtsp_cam_1}` là id của Airflow Connection giữ
  Login và Password, lưu mã hóa bằng `fernet_key`.
- Máy mới phải tạo lại các Connection trên web (Admin → Connections, kiểu Generic), hoặc
  dùng `airflow connections add rtsp_cam_1 --conn-type generic --conn-login admin --conn-password …`.
- File còn mật khẩu viết thẳng thì chuyển bằng
  `scripts/migrate_rtsp_credentials.py` (mặc định chạy thử; `--apply` làm thật).
  Che mật khẩu trong log cũ bằng `--redact-logs --apply`.
- **Không bao giờ in URL RTSP ra log mà không bọc `redact()`.** Logger riêng của pipeline
  (`logs/pipeline.log`) ghi thẳng ra file, không đi qua bộ che secret của Airflow.
  `RedactRtspFilter` đã gắn vào logger này, nhưng `print()` thì không được lọc.

## Không được làm

- Commit `.env`, `cookies/`, `gemini_keys.json`, `airflow.cfg`, `rtsp/*.txt` hay
  `data/db/`. Tất cả đã nằm trong `.gitignore`; kiểm tra lại bằng
  `git status` trước khi commit.
- Viết cứng đường dẫn `/home/<user>/...` vào code. Hãy tính từ `__file__` và cho phép
  ghi đè bằng biến môi trường (mẫu: `_ext()` trong `src/modules/config.py`).
