# CLAUDE.md

Tổng quan, cài đặt, vận hành: xem `README.md`. Hướng dẫn từng loại việc nằm trong
`.claude/skills/`: `setup-machine`, `add-platform`, `add-filter`, `manage-labels`,
`test-pipeline` và `<platform>-dag`.

## Bất biến: phá vỡ là hỏng dataset

- **Mọi URL phải qua `crawler_core.pipeline.VideoPipeline`.** Scraper chỉ yield
  `(urls, nguồn, nhãn)` và không tự tải.
- **Status `downloaded` / `skipped_*` là trạng thái cuối.** L0 không bao giờ xử lý lại
  URL mang các status này. Vì vậy **lỗi hệ thống (GPU, mạng) phải ghi `failed`, không
  được ghi `skipped_*`**.
- **Test không được ghi vào `data/db/tracker.db`.** Dùng
  `scripts/test_pipeline_offline.py` hoặc Chế độ test (`test_mode`).
- **Task nạp model lên GPU phải nằm trong pool `social_crawler_gpu`** (1 slot, GPU 6 GB).

## Quy ước

- Comment và log viết **tiếng Việt**, giải thích **vì sao** kèm số đo thực tế và ngày đo.
  Giữ nguyên phong cách này.
- Mọi tham số đều ghi đè được bằng biến môi trường: crawler ở
  `platform_crawlers/config.py`, VideoMAE ở `crawler_core/videomae_filter.py`. Không
  viết cứng đường dẫn máy; tính từ `__file__`.
- Bộ lọc bật/tắt được từ giao diện qua `FILTERS` + `resolve_filters()` trong
  `dags/social_crawler_common.py`.
- Python 3.10, Airflow 3.1.6 (Task SDK: `from airflow.sdk import Param, Variable`).
  Chạy lệnh bằng `airflow_venv/bin/python` với `AIRFLOW_HOME` = thư mục dự án.
- Kiểm tra sau khi sửa DAG: `airflow_venv/bin/python scripts/doctor.py`, bước cuối parse
  mọi DAG.
