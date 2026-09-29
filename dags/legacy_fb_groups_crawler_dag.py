"""
DAG legacy_fb_groups_crawler — BẢN CŨ: tải video trong CÁC NHÓM Facebook mà tài khoản đã
tham gia (không search theo nhãn), lưu hết vào 1 nhãn chung FB_CATEGORY='CCTV'. Không
nằm trong vòng xoay. Đã thay bằng social_crawler_fb (search tab "Thước phim" theo 27 nhãn);
giữ lại để quét nhóm khi cần. Đăng nhập: cookie cookies/facebook_legacy_cookies.*, tự
lấy từ Chrome qua platform_crawlers/sessions.py (đăng nhập facebook.com trong Chrome).

Tên cũ (trước 2026-09-28): fb_cctv_video_crawler, file dags/fb_video_crawler_dag.py.

Pipeline:
  1. crawl_all_groups  — Với mỗi nhóm: lấy 100 URL → đẩy qua VideoPipeline
  2. report_stats      — In thống kê tổng kết

Luồng xử lý per-group:
  Group A → [Batch 1: 100 URL → pipeline] → [Batch 2: ...] → hết → Group B → ...

Dedup: DAG này KHÔNG tự làm dedup nữa — mọi URL đi qua
crawler_core.pipeline.VideoPipeline, đúng cùng một cổng mà
5 DAG social_crawler_* dùng cho YouTube/Dailymotion/Reddit/X/Facebook

  L0 URL đã xử lý → L1 canonical ID → download (staging)
  → 9:16 filter → L2 file hash → L3 thumbnail phash → L4 5-frame fingerprint
  → phân loại CCTV → dataset  (video bị loại: xem CRAWL_REJECT_ACTION trong pipeline.py)

Nhờ dùng chung fingerprint store, một clip CCTV đã tải từ TikTok sẽ bị L3/L4
chặn khi gặp lại trên Facebook (và ngược lại).

Schedule: mỗi 24 giờ.
Đăng nhập: cookie Facebook lấy từ Chrome (platform_crawlers/sessions.py)
Classifier: load_classifier() — CLIP mặc định, CRAWL_CCTV_CLASSIFIER=qwen để dùng Qwen2.5-VL.
Deep scan:  FB_DEEP_SCAN=true, hoặc trigger với conf {"deep_scan": true}
"""

import os
import sys
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

# Nạp biến môi trường từ .env (Airflow không tự load) — FB_HEADLESS, FB_BATCH_SIZE, v.v.
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(PROJECT_ROOT, '.env'))
except Exception:
    pass

logger = logging.getLogger(__name__)

HEADLESS   = os.environ.get('FB_HEADLESS', 'true').lower() == 'true'
BATCH_SIZE = int(os.environ.get('FB_BATCH_SIZE', '100'))
CATEGORY   = os.environ.get('FB_CATEGORY', 'CCTV')
# FB_DEEP_SCAN=true  → scroll hết đáy nhóm, thu video cũ chưa từng lấy
# FB_DEEP_SCAN=false → chỉ bắt video mới ở đầu (default, nhanh)
# Override khi trigger thủ công từ Airflow UI: {"deep_scan": true}
_ENV_DEEP_SCAN = os.environ.get('FB_DEEP_SCAN', 'false').lower() == 'true'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=12),
}


# ---------------------------------------------------------------------------
# Task 1 — Crawl từng nhóm, mỗi batch đẩy qua cổng dedup chung
# ---------------------------------------------------------------------------

def task_crawl_all_groups(**context):
    from platform_crawlers.facebook_groups.fb_session import FBSession
    from platform_crawlers.facebook_groups.fb_group_media import FBGroupMediaScraper
    from crawler_core.facebook_downloader import VideoDownloader
    from crawler_core.db_manager import DBManager
    from crawler_core.pipeline import VideoPipeline, load_classifier
    from platform_crawlers import sessions

    # Làm mới cookie từ Chrome (ghi cả cookies/facebook_legacy_cookies.* mà FBSession đọc)
    s = sessions.ensure_session('facebook')
    if s.blocking:
        raise RuntimeError(f'Facebook: {s.message}')

    db         = DBManager()
    classifier = load_classifier()      # CLIP (mặc định) | qwen — CRAWL_CCTV_CLASSIFIER
    downloader = VideoDownloader()      # FB cần DASH capture riêng, không dùng GenericDownloader

    # Cùng một cổng mà 5 DAG social_crawler_* dùng — dedup dùng chung store
    pipeline = VideoPipeline(
        db=db,
        downloader=downloader,
        classifier=classifier,
        platform='facebook',
        category=CATEGORY,
        skip_portrait=True,
    )

    # deep_scan bật được từ 3 nơi (ưu tiên cao → thấp):
    #   1. Trigger conf: Trigger DAG w/ config → {"deep_scan": true}
    #   2. Env var:      FB_DEEP_SCAN=true
    #   3. Default:      false
    dag_run   = context.get('dag_run')
    conf_deep = (dag_run.conf or {}).get('deep_scan', False) if dag_run else False
    deep_scan = bool(conf_deep) or _ENV_DEEP_SCAN

    with FBSession(headless=HEADLESS) as session:
        scraper = FBGroupMediaScraper(session)
        groups  = scraper.get_joined_groups()
        logger.info(
            f"Tìm thấy {len(groups)} nhóm — deep_scan={deep_scan}, "
            f"fingerprint đã có: {db.fingerprint_count()}"
        )

        for group_url in groups:
            known_urls    = db.get_known_urls_for_source(group_url)
            group_batches = 0

            logger.info(
                f"\n{'='*55}\n"
                f"Nhóm: {group_url}\n"
                f"  URL đã biết: {len(known_urls)}\n"
                f"{'='*55}"
            )

            for batch, is_last in scraper.iter_group_videos(
                group_url,
                batch_size=BATCH_SIZE,
                known_urls=known_urls,
                deep_scan=deep_scan,
            ):
                if not batch:
                    break

                group_batches += 1
                logger.info(
                    f"[{group_url}] Batch #{group_batches}: {len(batch)} URL — xử lý..."
                )

                # Toàn bộ dedup 4 tầng + filter + download nằm trong pipeline
                outcome = pipeline.process_batch(batch, source_page=group_url)
                logger.info(
                    f"  → {outcome} | lũy kế: {pipeline.stats.summary()}"
                )

                if is_last:
                    break

            db.add_source(group_url, 'group', category='unknown')
            db.update_source_scraped(
                group_url,
                scan_complete=(not deep_scan or group_batches > 0),
            )
            logger.info(f"Nhóm [{group_url}] xong sau {group_batches} batch")

    logger.info(f"\nTổng kết crawl Facebook: {pipeline.stats.summary()}")
    return {'platform': 'facebook', **pipeline.stats.as_dict()}


# ---------------------------------------------------------------------------
# Task 2 — Report tổng kết từ DB
# ---------------------------------------------------------------------------

def task_report_stats(**context):
    from crawler_core.db_manager import DBManager

    db    = DBManager()
    stats = db.get_stats()

    logger.info('=' * 55)
    logger.info('CRAWLER STATS')
    logger.info('=' * 55)
    logger.info(f"URL statuses   : {stats['urls']}")
    logger.info(f"Downloaded/cat : {stats['categories']}")
    logger.info(f"Fingerprints   : {stats['fingerprints']}")
    logger.info(f"Theo platform  : {stats['platforms']}")
    logger.info('=' * 55)
    return stats


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id='legacy_fb_groups_crawler',
    description='[CŨ] Tải video trong các nhóm Facebook đã tham gia → VideoPipeline (nhãn chung CCTV). Bản mới: social_crawler_fb',
    default_args=default_args,
    start_date=datetime(2026, 6, 18),
    schedule='0 */24 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['legacy', 'facebook', 'video_crawler'],
) as dag:

    crawl_all_groups = PythonOperator(
        task_id='crawl_all_groups',
        python_callable=task_crawl_all_groups,
    )

    report_stats = PythonOperator(
        task_id='report_stats',
        python_callable=task_report_stats,
        trigger_rule='all_done',
    )

    crawl_all_groups >> report_stats
