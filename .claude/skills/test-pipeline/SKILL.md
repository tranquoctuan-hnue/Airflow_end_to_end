---
name: test-pipeline
description: Chạy thử pipeline crawler hoặc vòng xoay DAG mà KHÔNG ghi vào tracker.db thật, dataset hay trạng thái xoay nhãn — test offline trên video có sẵn (không cần mạng), Chế độ test trên Airflow, test vòng xoay nhỏ qua REST API. Dùng sau khi sửa bộ lọc/scraper/DAG, khi người dùng muốn "chạy thử", "test", hoặc kiểm tra một thay đổi trước khi chạy thật.
---

# Chạy thử an toàn

Ba mức, từ nhanh đến sát thực tế. **Không bao giờ test bằng cách chạy thật vào
`data/db/tracker.db`**: một URL đã bị ghi `skipped_*` sẽ bị L0 bỏ qua vĩnh
viễn.

## 1. Offline: bộ lọc trên video có sẵn (không mạng, không Airflow)

```bash
airflow_venv/bin/python scripts/test_pipeline_offline.py data/videos/*.mp4
airflow_venv/bin/python scripts/test_pipeline_offline.py <thư_mục> --category Robbery --no-videomae
```

- Chạy **VideoPipeline thật** (9:16, L2–L4, CLIP, VideoMAE); chỉ thay yt-dlp bằng một
  lớp chép file.
- DB tạm và kết quả nằm ở `data/.test_runs/offline_<thời điểm>/`.
- `--real-db` dedup với **bản sao** lịch sử thật.
- Video trong `data/videos/` có file `.json` nhãn tay theo từng đoạn 5 giây, và script in
  nhãn tay ra để so với kết quả bộ lọc.

Đây là cách chính để đo bộ lọc mới: chạy trên cùng một bộ video trước và sau khi sửa.

## 2. Chế độ test trên Airflow (một DAG, dữ liệu thật từ mạng)

Giao diện web → DAG `social_crawler_<platform>` → **Trigger**:
- tick **"Chế độ test (không ghi DB thật)"**;
- "Trần thời gian" = 1–2 phút;
- "Tối đa URL mỗi nhãn" = 3, "Tối đa URL mỗi từ khóa" = 5.

Mỗi lượt tạo một thư mục `data/.test_runs/<thời điểm>_<platform>/` gồm: bản sao
`tracker.db` (dedup giống thật), `video/`, `video_rejected/` và bản sao
`label_cursor.json`. Log đầu lượt có dòng `PLATFORM: … [CHẾ ĐỘ TEST — …]`.
Code nằm ở `_make_sandbox()` trong `dags/social_crawler_common.py`.

## 3. Vòng xoay nhỏ (nhiều DAG nối nhau)

1. Đặt Variable `crawler_rotation` = `{"enabled": true, "order": ["youtube","x","reddit"]}`.
2. Unpause các DAG trong vòng.
3. Trigger DAG đầu tiên với: Chế độ test + **"Tiếp tục vòng xoay"** + trần 1 phút.
   Conf của lượt đầu (test_mode, các giới hạn, bộ lọc) được truyền nguyên cho các DAG kế
   tiếp.
4. **Khi DAG cuối vòng bắt đầu chạy**, đặt `"enabled": false`. Nếu không, vòng quay lại
   DAG đầu và chạy mãi.
5. Kỳ vọng:
   - mỗi DAG có 4 task, `crawl_*` → `pick_next_platform` → `trigger_next_platform`;
   - DAG cuối có `pick_next_platform = skipped`;
   - lượt tự động theo lịch `@daily` sinh ra khi Unpause có `crawl_* = skipped`, đúng
     thiết kế.
6. Dọn dẹp: Pause lại các DAG, trả `crawler_rotation` về như cũ, và xóa
   `data/.test_runs/` sau khi xem xong.

Làm qua REST API (không cần trình duyệt), lấy token bằng mật khẩu trong
`simple_auth_manager_passwords.json.generated`:
```python
tok = requests.post('http://localhost:8080/auth/token', json={'username': 'admin', 'password': pw}).json()['access_token']
H = {'Authorization': f'Bearer {tok}'}
requests.patch('http://localhost:8080/api/v2/dags/social_crawler_youtube', json={'is_paused': False}, headers=H)
requests.post('http://localhost:8080/api/v2/dags/social_crawler_youtube/dagRuns', headers=H, json={
    'logical_date': None, 'conf': {'test_mode': True, 'continue_rotation': True, 'time_budget_minutes': 1}})
```
Theo dõi trạng thái bằng cách đọc `airflow.db` ở chế độ chỉ đọc
(`sqlite3.connect('file:airflow.db?mode=ro', uri=True)`, bảng `dag_run` và
`task_instance`). Log task nằm ở `logs/dag_id=…/run_id=…/task_id=…/attempt=1.log`,
mỗi dòng là JSON và nội dung nằm ở khóa `event`.

## Trước khi test: kiểm tra những thứ có thể làm test chạy sai

- **Lượt treo cũ:** lượt `running` hoặc `up_for_retry` từ trước sẽ chạy crawl thật ngay
  khi Unpause. Chuyển chúng sang `failed` trước (PATCH `dagRuns/<id>` với
  `{"state": "failed"}`).
- **GPU đang bị chiếm:** kiểm tra `nvidia-smi`. Một tiến trình khác đang chạy model sẽ
  làm số đo thời gian sai lệch nhiều lần.
- **Sau khi test, xác nhận DB thật không bị đụng:** thời gian sửa của `tracker.db` và
  `state/label_cursor.json` phải không đổi, và không có URL test nào nằm trong DB thật.
