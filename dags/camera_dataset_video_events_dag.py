"""
DAG camera_dataset_video_events — xem camera đang phát (RTSP hoặc YouTube live) bằng model
VideoMAE multi-label 368x640, LƯU LẠI ĐOẠN 5 GIÂY khi có đầu ra > 40%.

Khác social_crawler_* (tải video từ mạng xã hội) và camera_dataset_* cũ (chụp ảnh): ở
đây kết quả là clip 5s + file annotation video_classification, đúng dạng clip train của
VideoMAE, để người gán nhãn duyệt rồi đưa vào dataset.

Xoay vòng: mỗi camera trong rtsp/cam_event.txt được xem CAMERA_EVENT_MINUTES phút
(mặc định 5) rồi chuyển camera kế; hết danh sách thì quay lại đầu. Vì sao không xem mọi
camera cùng lúc: ViT-L 368x640 trên GTX 1660 SUPER tốn 3,9s cho mỗi đoạn 5s (đo
2026-11-25) → GPU chỉ theo kịp 1 luồng. Chi tiết: src/modules/camera_event_recorder.py.

Mỗi lần chạy DAG xoay vòng CAMERA_EVENT_RUN_MINUTES phút (mặc định 60, xong camera đang
xem mới dừng) rồi kết thúc. Lịch '* * * * *' + max_active_runs=1: run mới được tạo
trong vòng 1 phút sau khi run trước xong → chạy gần như liên tục. Không dùng
'@continuous' vì khi lỗi cấu hình (thiếu model, file camera) nó tạo run mới NGAY lập
tức, lặp lỗi liên tục; lịch mỗi phút giới hạn còn 1 lần/phút.

Kết quả (thư mục đổi bằng CAMERA_EVENT_OUTPUT_DIR trong .env, đặt được là NAS):
data/camera_events/<camera>/<YYYY-MM-DD>/<HHMMSS>_<Nhãn>_<điểm>.mp4 + .json,
và data/camera_events/events.jsonl (mỗi đoạn đã lưu 1 dòng, đủ điểm 16 đầu ra).

Cài đặt (máy mới):
  1. Model: models/Video_understanding/vitl_368x640_ml16_231126/
     (checkpoint-best.pth bản gọn 1,1 GB + thresholds.json) — xem models/README.md.
  2. Camera: chép rtsp/cam_event.example.txt → rtsp/cam_event.txt.
  3. Tài khoản camera: Airflow Connection theo {conn_id} trong URL.
Biến môi trường: CAMERA_EVENT_* (đầu file camera_event_recorder.py), CAMERA_EVENT_FILE,
CAMERA_EVENT_RUN_MINUTES, CAMERA_EVENT_POOL.

Tạm dừng: airflow dags pause camera_dataset_video_events
"""

import logging
import os
import sys
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

logger = logging.getLogger('airflow.task.camera_dataset_video_events')  # xem camera_event_recorder.py

HCM_TZ = pendulum.timezone('Asia/Ho_Chi_Minh')

RUN_MINUTES = float(os.environ.get('CAMERA_EVENT_RUN_MINUTES') or 60)
MINUTES_PER_CAMERA = float(os.environ.get('CAMERA_EVENT_MINUTES') or 5)

# Task nạp ViT-L (1,5 GB VRAM) lên GPU → theo bất biến của dự án phải vào pool 1 slot
# chung với các DAG dùng GPU khác. ⚠ Task giữ slot suốt RUN_MINUTES: trong lúc đó các
# DAG GPU khác phải chờ. Máy chỉ chạy DAG này thì đặt CAMERA_EVENT_POOL=default_pool.
GPU_POOL = os.environ.get('CAMERA_EVENT_POOL') or 'social_crawler_gpu'


def task_record_events(**context):
    # Nạp .env lúc task chạy (như social_crawler_common): sửa CAMERA_EVENT_* trong .env
    # có hiệu lực từ run sau, không phải khởi động lại Airflow. Không ghi đè biến đã có.
    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(PROJECT_ROOT, '.env'))
    except Exception:
        pass
    from src.modules.camera_event_recorder import (
        MINUTES, OUTPUT_DIR, THRESHOLD, VideoMAEMultiLabel, rotate)
    from src.modules.config import RTSP_FILE
    from src.modules.rtsp_credentials import load_rtsp_urls

    cam_file = RTSP_FILE['event']
    if not os.path.exists(cam_file):
        raise FileNotFoundError(f'Không có {cam_file} — chép rtsp/cam_event.example.txt rồi sửa')
    urls = load_rtsp_urls(cam_file)
    if not urls:
        raise ValueError(f'{cam_file} không có camera nào dùng được')

    model = VideoMAEMultiLabel()
    logger.info(f'Model {model.name} trên {model.device}; {len(urls)} camera, mỗi camera '
                f'{MINUTES:g} phút, lưu khi đầu ra > {THRESHOLD:.0%} → {OUTPUT_DIR}')
    totals = rotate(urls, model, RUN_MINUTES)
    for cam, t in totals.items():
        logger.info(f'  {cam}: {t["visits"]} lượt, {t["segments"]} đoạn, lưu {t["saved"]}, '
                    f'bỏ {t["dropped"]}, {t["errors"]} lượt không mở được')
    if all(t['segments'] == 0 for t in totals.values()):
        # Lỗi hệ thống (mạng/camera) phải hiện là failed, không được im lặng "success"
        raise RuntimeError('Không camera nào mở được trong cả lần chạy')
    return totals


with DAG(
    dag_id='camera_dataset_video_events',
    description='Camera RTSP/YouTube live xoay vòng 5 phút/camera → VideoMAE 368x640 → lưu đoạn 5s có đầu ra > 40%',
    start_date=pendulum.datetime(2026, 11, 25, 0, 0, tz=HCM_TZ),
    schedule='* * * * *',
    catchup=False,
    max_active_runs=1,
    default_args={'owner': 'airflow', 'retries': 0},
    tags=['camera_dataset', 'rtsp', 'youtube', 'videomae'],
) as dag:

    record_events = PythonOperator(
        task_id='record_events',
        python_callable=task_record_events,
        pool=GPU_POOL,
        # Xoay vòng tới RUN_MINUTES rồi xem nốt camera đang dở (tối đa MINUTES_PER_CAMERA)
        execution_timeout=timedelta(minutes=RUN_MINUTES + MINUTES_PER_CAMERA + 15),
    )
