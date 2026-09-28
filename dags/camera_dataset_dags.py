"""
Nhóm DAG camera_dataset_* — lấy frame từ CAMERA ĐANG PHÁT (RTSP hoặc YouTube live) rồi
nhận diện để làm dataset ảnh có nhãn cho các model riêng (KHÔNG liên quan dataset video
sự việc của social_crawler_*).

  camera_dataset_vehicle_anpr       YOLO xe → cắt xe → PaddleOCR biển số
                                    ảnh+nhãn: data/vehicle/, biển số: data/anpr/
  camera_dataset_person_attributes  YOLO người → cắt người → PaddleClas thuộc tính
                                    ảnh+nhãn: data/person/, thuộc tính: data/attributes/
  camera_dataset_helmet             YOLO xe → cắt xe → YOLO mũ bảo hiểm trên từng xe
                                    chỉ lưu crop có mũ thường, hoặc ≥2 lớp cùng lúc
                                    (mũ BH / mũ trùm / không mũ / điện thoại) → data/helmet/

Danh sách camera: RTSP_FILE trong src/modules/config.py (rtsp/cam_vh_lp.txt,
cam_person.txt, cam_vh_helmet.txt — không có trong git vì chứa địa chỉ camera). Mỗi dòng
1 URL RTSP dạng rtsp://{rtsp_cam_N}@host/... hoặc link YouTube live (video_capture.py
tự đổi sang luồng phát bằng yt-dlp). Nguồn link YouTube live: DAG camera_source_youtube_live.

Tên cũ (trước 2026-09-28): get_data_vehicle_anpr / get_data_person_attributes /
get_data_helmet_attributes, file dags/video_processing_pipelines.py — lịch sử chạy cũ vẫn
nằm trong airflow.db dưới tên cũ.
"""

import sys
import os
import time

from pathlib import Path

# Thư mục gốc dự án (suy ra từ vị trí file này — chạy được trên mọi máy)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)


# Setup CUDA/GPU environment for PaddleOCR
os.environ['CUDA_VISIBLE_DEVICES'] = '0'
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.modules.cam_processing import process_cameras_sequential, process_cameras_parallel
from src.pipeline.tasks.cam_frame_processing import process_vehicle_anpr_cam, process_person_attributes_cam, process_helmet_attributes_cam
from src.modules.config import RUN_MODE, RTSP_FILE

# from sam3.model_builder import build_sam3_image_model
# from sam3.model.sam3_image_processor import Sam3Processor

# Default DAG arguments
default_args = {
    # 'owner': 'airflow',
    # 'retries': 1,
    'retry_delay': timedelta(minutes=1),
    "catchup": False,
    'start_date': datetime(2024, 1, 1),
}

def _read_rtsp_urls(rtsp_file):
    # Tài khoản camera lấy từ Airflow Connection ({conn_id} trong URL) và được che
    # trong log — xem src/modules/rtsp_credentials.py
    from src.modules.rtsp_credentials import load_rtsp_urls
    return load_rtsp_urls(rtsp_file)

# ============================================================================
# DAG 1: camera_dataset_vehicle_anpr — biển số xe
# ============================================================================
with DAG(
    dag_id='camera_dataset_vehicle_anpr',
    default_args=default_args,
    description='Camera RTSP/YouTube live → YOLO xe → cắt → PaddleOCR biển số → data/vehicle, data/anpr',
    schedule=None,  # Manual trigger
    catchup=False,
    tags=['camera_dataset', 'rtsp', 'anpr', 'yolo', 'paddleocr'],
) as dag_vehicle_anpr:

    def vehicle_anpr():
        """Process all URLs with 4-step pipeline."""
        rtsp_file_path = RTSP_FILE.get('vehicle_lp') if isinstance(RTSP_FILE, dict) else RTSP_FILE
        urls = _read_rtsp_urls(rtsp_file_path)

        if RUN_MODE == 'sequential':
            process_cameras_sequential(urls, process_vehicle_anpr_cam, max_frames=5)
        elif RUN_MODE == 'parallel':
            process_cameras_parallel(urls, process_vehicle_anpr_cam, cooldown_sec=60)
        else:
            print(f"Unknown RUN_MODE: {RUN_MODE}")

    task_4_step = PythonOperator(
        task_id='capture_detect_ocr',
        python_callable= vehicle_anpr,
    )

# Export DAG for Airflow to discover
camera_dataset_vehicle_anpr = dag_vehicle_anpr

# ============================================================================
# DAG 2: camera_dataset_person_attributes — thuộc tính người
# ============================================================================
with DAG(
    dag_id='camera_dataset_person_attributes',
    default_args=default_args,
    description='Camera RTSP/YouTube live → YOLO người → cắt → PaddleClas thuộc tính → data/person, data/attributes',
    schedule=None,  # Manual trigger
    catchup=False,
    tags=['camera_dataset', 'rtsp', 'person', 'yolo', 'paddleclas'],
) as dag_person_attributes:

    def person_attributes():
        """Process all URLs with 4-step pipeline."""
        rtsp_file_path = RTSP_FILE.get('person') if isinstance(RTSP_FILE, dict) else RTSP_FILE
        urls = _read_rtsp_urls(rtsp_file_path)

        if RUN_MODE == 'sequential':
            process_cameras_sequential(urls, process_person_attributes_cam, max_frames=60)
        elif RUN_MODE == 'parallel':
            process_cameras_parallel(urls, process_person_attributes_cam, cooldown_sec=60)
        else:
            print(f"Unknown RUN_MODE: {RUN_MODE}")

    task_person_attributes = PythonOperator(
        task_id='capture_detect_attributes',
        python_callable=person_attributes,
    )

# Export DAG for Airflow to discover
camera_dataset_person_attributes = dag_person_attributes

# ============================================================================
# DAG 3: camera_dataset_helmet — mũ bảo hiểm (bản SAM3 cũ đang comment bên dưới)
# ============================================================================
with DAG(
    dag_id='camera_dataset_helmet',
    default_args=default_args,
    description='Camera RTSP/YouTube live → YOLO xe → cắt → YOLO mũ bảo hiểm → data/helmet (7h–17h mỗi ngày)',
    schedule = "0 0 * * *",  # 7h AM UTC+7 daily
    catchup=False,
    tags=['camera_dataset', 'rtsp', 'helmet', 'yolo'],
) as dag_helmet_attributes:

    def helmet_attributes():

        # Try to read from vehicle_lp or vehicle RTSP file
        # rtsp_file_path = RTSP_FILE.get('helmet') if isinstance(RTSP_FILE, dict) else RTSP_FILE
        # if not rtsp_file_path:
        #     print("No vehicle RTSP file configured")
        #     return
        
        # urls = _read_rtsp_urls(rtsp_file_path)
        
        # Initialize SAM3 model (once per DAG run for efficiency)
        # try:
            
            
        #     print("Loading SAM3 model...")
        #     bpe_path = SAM3_CONFIG.get('bpe_path')
        #     sam3_device = SAM3_CONFIG.get('device', 'cuda:0')
        #     iou_threshold = SAM3_CONFIG.get('iou_threshold', 0.5)
        #     min_crop_size = SAM3_CONFIG.get('min_crop_size', 250)
        #     sam3_model = build_sam3_image_model(bpe_path=bpe_path, device=sam3_device, compile=SAM3_CONFIG.get('compile', True))
        #     sam3_model = sam3_model.to(sam3_device)
        #     processor = Sam3Processor(
        #         sam3_model, 
        #         resolution=SAM3_CONFIG.get('resolution', 672),
        #         device=sam3_device,
        #         confidence_threshold=SAM3_CONFIG.get('confidence_threshold', 0.6)
        #     )
        #     print("✓ SAM3 model loaded successfully")
        # except Exception as e:
        #     print(f"✗ Failed to load SAM3 model: {e}")
        #     import traceback
        #     traceback.print_exc()
        #     return

        start_time = time.time()
        while time.time() - start_time < 10 * 3600:  # chạy 10 tiếng (7h–17h), timeout task 12h
            rtsp_file_path = RTSP_FILE.get('helmet') if isinstance(RTSP_FILE, dict) else RTSP_FILE
            urls = _read_rtsp_urls(rtsp_file_path)

            if RUN_MODE == 'sequential':
                process_cameras_sequential(urls, process_helmet_attributes_cam, max_frames=30)
            elif RUN_MODE == 'parallel':
                process_cameras_parallel(urls, process_helmet_attributes_cam, cooldown_sec=60)
            else:
                print(f"Unknown RUN_MODE: {RUN_MODE}")

    task_helmet_detection = PythonOperator(
        task_id='capture_detect_helmet',
        python_callable=helmet_attributes,
        execution_timeout=timedelta(hours=12)
    )

# Export DAG for Airflow to discover
camera_dataset_helmet = dag_helmet_attributes

