"""
DAG camera_source_youtube_live — TÌM NGUỒN CAMERA: luồng CCTV đang phát trực tiếp trên
YouTube. KHÔNG tải video (khác social_crawler_youtube). Kết quả là danh sách link live đã
xác minh, dùng làm nguồn camera cho nhóm DAG camera_dataset_* (hiện phải chép tay link
sang rtsp/*.txt — chưa tự nối).

Tên cũ (trước 2026-09-28): youtube_cctv_live_crawler, file dags/youtube_cctv_crawler_dag.py.

Pipeline:
  1. find_and_snapshot_streams — chạy LIÊN TỤC 12 tiếng (18h -> 6h sáng giờ HCM),
     lặp lại các vòng liên tiếp KHÔNG nghỉ giữa các vòng (vòng này xong là vào vòng
     tiếp theo ngay):
       a. Search YouTube (yt-dlp) theo các từ khóa CCTV-style, lọc video đang live.
       b. Lọc rẻ theo tiêu đề (title_filter) — loại video giải trí/tổng hợp rõ ràng
          không phải footage camera thật, trước khi tốn chi phí capture.
       c. Bỏ qua nếu kênh đã từng đăng đúng title này trước đó (kênh ngắt live 24h rồi
          phát lại thành live mới với url khác nhưng cùng camera — xem snapshot_store.py).
       d. Với mỗi luồng MỚI: chụp 1 frame, xác minh lại bằng CCTVClassifier (Qwen2.5-VL
          local, crawler_core/qwen_classifier.py), rồi mới ghi (link + đường dẫn ảnh)
          vào file txt.
  2. report_stats — Sau khi task 1 kết thúc (hết 12 tiếng), đọc lại file txt,
     log tổng số luồng đã ghi nhận.

Schedule: 18:00 giờ Hồ Chí Minh, mỗi ngày. Mỗi lần chạy kéo dài 12 tiếng liên tục
(tới ~6h sáng hôm sau) rồi tự dừng, hôm sau 18h lại chạy tiếp.
Từ khóa search: youtube_live/config.py (SEARCH_KEYWORDS).
Tắt bước xác minh bằng model (chỉ lọc theo tiêu đề, không cần GPU):
  env YOUTUBE_CCTV_USE_CLASSIFIER=false
Kết quả: data/videos/youtube_cctv/live_streams.txt + snapshots/
"""

import os
import sys
import time
import logging
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

logger = logging.getLogger(__name__)

HCM_TZ = pendulum.timezone('Asia/Ho_Chi_Minh')

RUN_DURATION = timedelta(hours=12)

# DAG này cũng nạp Qwen2.5-VL lên GPU (CCTVClassifier trong
# crawler_core/qwen_classifier.py) nên PHẢI nằm cùng pool 1 slot với 5 DAG social_crawler_*,
# nếu không nó chạy song song và OOM GPU — pool chỉ chặn được task nào KHAI
# BÁO pool, không tự suy ra được task nào dùng GPU.
# ⚠ Task này giữ slot LIÊN TỤC 12 TIẾNG (18h→6h) nên trong khoảng đó 5 platform
# crawler sẽ kẹt `queued`, không xoay vòng được. Nếu ưu tiên crawl liên tục thì
# pause DAG này: airflow dags pause camera_source_youtube_live
GPU_POOL = 'social_crawler_gpu'
USE_CLASSIFIER = os.environ.get(
    'YOUTUBE_CCTV_USE_CLASSIFIER', 'true'
).lower() == 'true'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(minutes=30),
}


# ---------------------------------------------------------------------------
# Task 1 — Chạy liên tục RUN_DURATION, search + snapshot lặp vòng liên tiếp không nghỉ
# ---------------------------------------------------------------------------

def task_find_and_snapshot_streams(**context):
    from youtube_live.live_stream_finder import find_live_streams
    from youtube_live.snapshot_store import SnapshotStore

    store = SnapshotStore()
    deadline = time.monotonic() + RUN_DURATION.total_seconds()

    round_num = 0
    total_found = total_known = 0

    while True:
        round_num += 1
        candidates = find_live_streams()

        known = 0
        for stream in candidates:
            if store.has(stream['url']):
                known += 1
                continue
            store.process(
                stream['url'], stream['video_id'], stream['title'],
                stream.get('channel_id', ''),
            )

        total_found += len(candidates)
        total_known += known
        logger.info(
            f"[Round {round_num}] found={len(candidates)} known={known} | "
            f"tổng lũy kế -> new={store.stats['added']} "
            f"capture_failed={store.stats['capture_failed']} "
            f"rejected_not_cctv={store.stats['rejected_not_cctv']} "
            f"duplicate_broadcast={store.stats['duplicate_broadcast']}"
        )

        if time.monotonic() >= deadline:
            break

    logger.info(f"Hoàn tất {round_num} vòng sau {RUN_DURATION}: {store.stats}")
    return {'rounds': round_num, 'found_total': total_found, 'known_total': total_known, **store.stats}


# ---------------------------------------------------------------------------
# Task 2 — Report tổng số luồng đã ghi nhận trong file txt
# ---------------------------------------------------------------------------

def task_report_stats(**context):
    from youtube_live.config import OUTPUT_TXT_PATH

    total = 0
    if os.path.exists(OUTPUT_TXT_PATH):
        with open(OUTPUT_TXT_PATH, 'r', encoding='utf-8') as f:
            total = sum(1 for line in f if line.strip())

    logger.info(f"Tổng số luồng CCTV YouTube đã ghi nhận: {total} ({OUTPUT_TXT_PATH})")
    return {'total_recorded': total}


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id='camera_source_youtube_live',
    description='Tìm luồng CCTV đang live trên YouTube (18h–6h), chụp frame, xác minh Qwen2.5-VL → live_streams.txt',
    default_args=default_args,
    start_date=pendulum.datetime(2026, 7, 23, 0, 0, tz=HCM_TZ),
    schedule='0 18 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['camera_source', 'youtube', 'live', 'qwen'],
) as dag:

    find_and_snapshot_streams = PythonOperator(
        task_id='find_and_snapshot_streams',
        python_callable=task_find_and_snapshot_streams,
        execution_timeout=RUN_DURATION + timedelta(minutes=30),
        # Serialize GPU chéo DAG — xem ghi chú ở GPU_POOL đầu file.
        pool=GPU_POOL if USE_CLASSIFIER else 'default_pool',
    )

    report_stats = PythonOperator(
        task_id='report_stats',
        python_callable=task_report_stats,
        # find_and_snapshot_streams chạy liên tục 12h — nếu bị kill giữa chừng
        # (SIGINT do scheduler restart, OOM, v.v.) task instance có thể kẹt ở
        # state=None thay vì 'failed'. all_success (mặc định) sẽ khiến
        # report_stats chờ vô thời hạn và không bao giờ chạy. all_done đảm bảo
        # luôn có log tổng kết dù task trên thành công/thất bại/bị kill.
        trigger_rule='all_done',
    )

    find_and_snapshot_streams >> report_stats
