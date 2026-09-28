# models/

Model **không nằm trong git** (hàng GB). Sau khi clone, chép model từ máy gốc hoặc NAS
vào đúng các đường dẫn dưới đây. Mọi đường dẫn đều đổi được bằng biến môi trường trong
`.env`, nên model có thể nằm ở nơi khác.

Không có model nào ở đây thì crawler vẫn chạy được: chỉ cần tắt bộ lọc tương ứng.

## Bắt buộc cho crawler

| Model | Đường dẫn | Biến môi trường | Không có thì |
|---|---|---|---|
| VideoMAE ViT-L, 20 lớp (bộ lọc sự việc) | `Video_understanding/vitl_224x224_240926/checkpoint-best.pth` + `labels.txt` | `VIDEOMAE_MODEL_DIR` | đặt `SOCIAL_USE_VIDEOMAE=false` |
| CLIP ViT-B-32 (bộ lọc CCTV) | tự tải về `~/.cache/huggingface` ở lần chạy đầu (~600 MB) | — | cần internet ở lần đầu |
| Probe CCTV tự huấn luyện (tùy chọn) | `cctv_probe.joblib` | `CCTV_DETECTOR_PROBE` | chạy zero-shot |

`labels.txt` ghi mỗi dòng một tên lớp, **đúng thứ tự lớp lúc train**. Phải có một dòng
`Normal`.

### Chép VideoMAE cho nhẹ

`checkpoint-best.pth` lúc train chứa cả trạng thái optimizer (3,4 GB). Suy luận chỉ cần
trọng số, nên nên tạo bản gọn 1,1 GB trước khi chép. Kết quả suy luận giống hệt (đã kiểm
tra ngày 2026-09-28):

```bash
python scripts/slim_checkpoint.py \
    models/Video_understanding/vitl_224x224_240926/checkpoint-best.pth \
    /tmp/vitl/checkpoint-best.pth
cp models/Video_understanding/vitl_224x224_240926/labels.txt /tmp/vitl/
# rồi chép /tmp/vitl/ sang máy mới: models/Video_understanding/vitl_224x224_240926/
```

Bản gọn không train tiếp được, nên phải giữ bản gốc ở máy train.

Code kiến trúc model nằm sẵn trong repo, ở `crawler_core/third_party/videomae/`, nên máy
mới không cần clone repo VideoMAE.

## Chỉ cho pipeline RTSP (`dags/camera_dataset_dags.py`)

Đường dẫn khai báo trong `src/modules/config.py`:

| Model | Đường dẫn trong `models/` | Biến môi trường |
|---|---|---|
| YOLO phương tiện | `Yolo_detect/Vehicle/yolov11n_vh_640_170426/weights/best.pt` | — |
| YOLO người | `Yolo_detect/Person/yolov11n_human_640_0610252/weights/best.pt` | — |
| YOLO mũ bảo hiểm | `Yolo_detect/Helmet/yolov26m_320_helmet_190626/weights/best.pt` | — |
| ANPR phát hiện biển số | `ANPR/det/inference/` | `ANPR_DET_MODEL_DIR` |
| ANPR nhận dạng ký tự | `ANPR/rec/inference/` + `ANPR/dict.txt` | — |
| ANPR phân loại góc | `ANPR/ch_ppocr_mobile_v2.0_cls_infer/` | `ANPR_CLS_MODEL_DIR` |
| Thuộc tính người (PaddleClas) | `Attribute_person/inference/` + `Attribute_person/class_id_map.txt` | `PERSON_ATTR_MODEL_DIR`, `PERSON_ATTR_CLASS_MAP` |
| SAM3 BPE vocab | `sam3/bpe_simple_vocab_16e6.txt.gz` | `SAM3_BPE_PATH` |

Đặt `VIDEO_PIPELINE_MODELS_DIR` để trỏ cả thư mục `models/` sang nơi khác.
