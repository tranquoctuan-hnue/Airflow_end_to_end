---
name: manage-labels
description: Thêm, sửa mô tả, đổi tên hoặc gộp nhãn sự việc (Arson, Robbery, GunRobber, Collapse…) của dataset — ảnh hưởng tới từ khóa Gemini, thư mục lưu video, xoay nhãn theo thiếu hụt, và khớp nhãn với model VideoMAE. Dùng khi người dùng đưa danh sách nhãn mới, muốn gộp/bỏ nhãn, hoặc video bị xếp sai nhãn.
---

# Quản lý nhãn sự việc

Nguồn sự thật duy nhất: `platform_crawlers/labels.py` → `LABELS`, hiện có 27 nhãn khớp
`classes_full.txt` của người dùng.

```python
'GunRobber': {                                   # key = TÊN THƯ MỤC (CamelCase, không dấu cách)
    'display': 'Gun-toting robber',              # = đúng tên trong danh sách lớp gán nhãn
    'desc': ('a robbery where the robber clearly holds or points a GUN ...'),  # → prompt Gemini
    'en': ['cctv armed robbery gun store', ...], # từ khóa tĩnh khi Gemini lỗi / hết quota
},
```

## Quy tắc viết `desc` (quan trọng nhất)

Gemini sinh từ khóa tìm kiếm **chỉ dựa vào `desc`**. Hai nhãn gần nhau mà mô tả không
loại trừ nhau thì từ khóa sẽ trùng, và video bị xếp lẫn nhãn. Mọi nhãn gần nhau phải chỉ
rõ **"NOT …, that is <Nhãn khác>"**:

- Robbery ⊃ tách riêng GunRobber (thấy rõ súng), KnifeRobber (thấy rõ dao), Snatching
  (giật rồi chạy).
- Fighting (tay không) ≠ ArmedFight (có hung khí).
- Shooting (cảnh nổ súng) ≠ ShootDown (người trúng đạn gục xuống) ≠ ArmedSuspect (cầm
  vũ khí, chưa hành động).
- Stealing (lén lút, gồm cả Shoplifting cũ) ≠ Burglary (đột nhập) ≠ CarTheft/MotorTheft ≠
  Snatching.
- Collapse (gồm cả trượt/vấp ngã, tức FallOver cũ) ≠ Unconscious (nằm bất động).
- Smoke ≠ Arson/Explosion. Normal là lớp âm tính: bộ lọc VideoMAE tự bỏ qua nhãn này.

Khi thêm nhãn mới, sửa luôn `desc` của các nhãn cũ gần nó. Câu ví dụ trong prompt nằm ở
`platform_crawlers/keywords.py` (`_generate`, đoạn "for example Stealing is…"). Nếu nó
nhắc tới nhãn đã bị bỏ thì phải sửa.

Nhãn chưa rõ nghĩa (ví dụ "Smok", "Shoot down") thì **hỏi người dùng**, đừng đoán.

## Gộp hoặc bỏ nhãn

1. Xóa nhãn cũ khỏi `LABELS`, và mở rộng `desc` của nhãn đích để bao gồm nội dung nhãn cũ.
2. Chuyển dữ liệu cũ bằng `scripts/merge_labels.py`. Script này mặc định **chạy thử**;
   `--apply` mới làm thật và sao lưu DB trước khi sửa.
   ```bash
   airflow_venv/bin/python scripts/merge_labels.py --map Shoplifting=Stealing            # xem trước
   airflow_venv/bin/python scripts/merge_labels.py --map Shoplifting=Stealing --apply
   ```
   Script đổi `final_category` trong DB, chuyển file cả trong dataset lẫn
   `video_rejected/`, và sửa trường `category` trong file `.json` đi kèm. Chạy khi không
   có DAG crawler nào đang chạy.

**Vì sao phải đổi nhãn trong DB:** thứ tự crawl nhãn tính từ số video thực tế trong DB
(`DBManager.downloaded_by_label()` → `label_cursor.order_labels()`), nhãn ít video nhất
đi trước. Không gộp dữ liệu thì nhãn đích bị coi là mới tinh và được ưu tiên sai.

## Khớp với model VideoMAE

`labels.txt` của model dùng tên hiển thị (ví dụ `Gun-toting robber`, `Road Accident`).
`pipeline._same_label()` so khớp sau khi bỏ dấu cách/ký tự đặc biệt và chấp nhận tiền tố
(Smok ~ Smoke), đồng thời so với cả `display`. Kết quả chỉ dùng để tham khảo
(`matches_category`), không dùng để lọc.

**Nhãn crawler có nhưng model chưa được học thì gần như luôn bị bộ lọc VideoMAE loại.**
Hiện có 7 nhãn như vậy: Abuse, Drowning, PourPetrol, Riot, ShootDown, Stampede, Snatching.
Khi thêm nhãn mới, báo người dùng điều này. Hướng xử lý: tắt `videomae` cho các lượt crawl
nhãn đó, hoặc train lại model.

## Kiểm tra

```bash
CRAWL_LABELS="Nhãn mới|Robbery" airflow_venv/bin/python -c "
import sys; sys.path.insert(0,'.')
from platform_crawlers import labels as L, keywords as K
print(L.enabled_labels()); print(K._generate(L.enabled_labels(), as_hashtag=False))"
```

Gọi Gemini thật: phải ra từ khóa cho đủ số nhãn, và từ khóa của các nhãn gần nhau không
trùng nhau.
