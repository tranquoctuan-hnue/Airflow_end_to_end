---
name: add-filter
description: Thêm, sửa hoặc đổi thứ tự một điều kiện lọc video trong VideoPipeline (ví dụ lọc theo độ phân giải, thời lượng, model mới, OCR, phát hiện watermark) — kèm ghi lý do loại, chuyển video bị loại sang thư mục riêng, công tắc bật/tắt theo platform trên giao diện Airflow. Dùng khi người dùng muốn thêm/bỏ/thay điều kiện lọc hoặc thay model phân loại.
---

# Thêm một bộ lọc

Mọi bộ lọc nằm trong `crawler_core/pipeline.py` → `VideoPipeline._filter_and_store()`.
Hàm này chạy **sau khi video đã tải về staging** (ổ nội bộ). Thứ tự hiện tại:

```
9:16 → L2 SHA-256 → L3 phash → L4 fingerprint → CLIP (CCTV) → VideoMAE (sự việc) → nhận
```

Sắp theo **rẻ trước, đắt sau**. Bộ lọc mới đặt ở vị trí theo chi phí của nó; bộ lọc chạy
model thì đặt sau dedup, để video trùng không tốn GPU. Việc loại dựa trên URL/metadata
(chưa cần file) thì đặt ở `_process()`, **trước** `downloader.fetch()`. Nhưng khi đó
không còn file để xem lại, nên chỉ làm vậy với điều kiện chắc chắn đúng, như L0/L1.

## Checklist: bỏ sót bước nào là mất tính năng tương ứng

**A. `crawler_core/pipeline.py`**
1. Hằng outcome, ví dụ `LOW_RES = 'low_res'`. Giá trị này cũng là **tên thư mục** trong
   `video_rejected/`.
2. `_STATUS_MAP[LOW_RES] = 'skipped_…'`. Status phải nằm trong `_TERMINAL_STATUSES`,
   nếu không lượt sau sẽ tải lại mãi. Có thể dùng lại `skipped_not_cctv` như 9:16 đang
   làm, vì lý do chính xác đã nằm ở cột `reject_reason`.
3. `_REASON_TEXT[LOW_RES] = '<câu giải thích tiếng Việt>'`. Câu này được ghi vào file
   `.json` đi kèm video bị loại.
4. `PipelineStats._KEYS` và `summary()`: thêm bộ đếm để log tổng kết hiện số video bị
   bộ lọc này loại.
5. Tham số mới trong `__init__`, ví dụ `min_height: int = 0` hoặc
   `quality_filter=None` (None = tắt), cộng khóa trong `checks['filters_enabled']`.
6. Logic trong `_filter_and_store()`:
   ```python
   if self.min_height:
       dims = dedup.video_dimensions(staged) or (0, 0)
       checks['low_res'] = {'height': dims[1], 'min': self.min_height}   # luôn ghi, cả khi đạt
       if dims[1] < self.min_height:
           return self._reject(ctx, LOW_RES, checks, staged)            # tự move/xóa + ghi DB
   ```
   - **Luôn ghi kết quả vào `checks`, kể cả khi video đạt.** `checks` được lưu thành
     `filter_detail`, dùng để đánh giá độ chính xác của bộ lọc về sau.
   - Nếu có ảnh minh họa (frame model đã xem), truyền vào `_reject(..., frame)` để nó
     được chuyển kèm video.
   - **Bộ lọc bị lỗi (GPU, không đọc được file) KHÔNG được coi là "loại".** Ghi
     `self._finish(url, ERROR, checks=checks)` rồi `return ERROR` (status `failed`), để
     lượt sau thử lại. Coi lỗi là loại thì video tốt bị loại vĩnh viễn: đây đúng là lỗi
     đã xảy ra với Qwen trước đây.

**B. `dags/social_crawler_common.py`** (công tắc trên giao diện)
7. `FILTERS['low_res'] = '<nhãn hiện trên form Trigger>'`. Form Trigger tự có ô
   Bật/Tắt, và Variable `crawler_filters` tự nhận khóa này.
8. `resolve_filters()`: giá trị mặc định trong `enabled`, đọc từ biến môi trường, ví dụ
   `os.environ.get('SOCIAL_USE_LOWRES', 'true')`.
9. Trong `task_crawl_platform()`, truyền vào `VideoPipeline(...)` theo
   `filters['low_res']`. Model nặng chỉ nạp khi bộ lọc bật (mẫu: khối `event_filter`).

**C. Tài liệu:** thêm biến môi trường vào `.env.example`, `start_airflow.sh` và bảng
luồng trong `README.md`. Nếu bộ lọc chạy model, ghi đường dẫn model vào
`models/README.md` và thêm kiểm tra vào `scripts/doctor.py`.

## Model mới: mẫu đã có sẵn

- `crawler_core/videomae_filter.py`, class `VideoMAEEventFilter.assess_video(path) -> dict`:
  - nạp checkpoint bằng mmap;
  - đọc cấu hình từ `ckpt['args']`;
  - chấm theo đoạn 5 giây;
  - đọc frame bằng ffmpeg tuần tự, thu nhỏ lúc giải mã. **Không dùng decord `get_batch`**: nó tự giải mã cả video ở độ phân giải gốc vào RAM (video 12 phút 1080p → OOM 13,7 GB, 2026-09-29). Bộ lọc mới nào cũng phải đo RAM với video dài;
  - tự giảm batch khi hết VRAM.
- Model cần GPU thì task vẫn nằm trong pool `social_crawler_gpu`, nên không cần làm gì
  thêm. **Đừng nạp hai model lớn cùng lúc** trên GPU 6 GB.

## Kiểm tra

Chạy offline bằng downloader giả trên video mẫu có nhãn tay (`data/videos/*.mp4` kèm
`.json`), xem skill `test-pipeline`. Sau đó chạy một lượt Chế độ test thật. Tiêu chí:
- video bị loại nằm đúng thư mục `video_rejected/<outcome>/<nhãn>/`;
- file `.json` có đủ `filters.<khóa>`;
- DB có `reject_reason = <outcome>`;
- log tổng kết có bộ đếm mới;
- tắt bộ lọc trên form Trigger thì log ghi `lọc <khóa>: TẮT (form Trigger)`.
