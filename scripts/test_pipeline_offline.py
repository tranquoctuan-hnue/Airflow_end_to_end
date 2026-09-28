"""
Chạy VideoPipeline THẬT (dedup + mọi bộ lọc) trên video có sẵn trên máy — không cần
mạng, không đụng tracker.db thật hay dataset. Dùng khi phát triển / chỉnh bộ lọc.

    airflow_venv/bin/python scripts/test_pipeline_offline.py data/videos/*.mp4
    airflow_venv/bin/python scripts/test_pipeline_offline.py thu_muc_video/ --category Robbery
    airflow_venv/bin/python scripts/test_pipeline_offline.py a.mp4 --no-cctv --no-videomae
    airflow_venv/bin/python scripts/test_pipeline_offline.py a.mp4 --real-db   # dedup với lịch sử thật (chỉ ĐỌC bản sao)

Mọi kết quả (DB tạm, video nhận / bị loại + .json lý do) nằm trong 1 thư mục mới dưới
data/.test_runs/offline_<thời điểm>/. Nếu video có file <tên>.json kiểu segment_infer.py
(nhãn tay theo đoạn) cạnh nó, nhãn tay được in ra để so.
"""

import argparse
import glob
import json
import logging
import os
import shutil
import sqlite3
import sys
import tempfile
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

VIDEO_EXTS = ('.mp4', '.mkv', '.webm', '.mov', '.avi')


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('inputs', nargs='+', help='file video hoặc thư mục')
    ap.add_argument('--category', default='RoadAccident', help='nhãn giả định (thư mục lưu)')
    ap.add_argument('--platform', default='youtube')
    ap.add_argument('--no-cctv', action='store_true', help='tắt bộ lọc CLIP')
    ap.add_argument('--no-videomae', action='store_true', help='tắt bộ lọc VideoMAE')
    ap.add_argument('--no-portrait', action='store_true', help='không loại video 9:16')
    ap.add_argument('--no-dedup', action='store_true', help='không loại video trùng L2–L4')
    ap.add_argument('--real-db', action='store_true',
                    help='dedup với BẢN SAO tracker.db thật (mặc định: DB trống)')
    ap.add_argument('-v', '--verbose', action='store_true')
    args = ap.parse_args()

    files = []
    for inp in args.inputs:
        if os.path.isdir(inp):
            files += sorted(p for p in glob.glob(os.path.join(inp, '**', '*'), recursive=True)
                            if p.lower().endswith(VIDEO_EXTS))
        elif os.path.isfile(inp):
            files.append(inp)
    if not files:
        sys.exit('Không tìm thấy video nào')

    work = os.path.join(ROOT, 'data', '.test_runs', f'offline_{datetime.now():%Y%m%d-%H%M%S}')
    os.makedirs(work)
    # Đặt env TRƯỚC khi import crawler_core (đường dẫn đọc lúc import module)
    os.environ.update(CRAWL_VIDEO_OUTPUT_DIR=os.path.join(work, 'video'),
                      CRAWL_REJECTED_DIR=os.path.join(work, 'video_rejected'),
                      CRAWL_DOWNLOAD_STAGING_DIR=os.path.join(work, 'staging'),
                      CRAWLER_DB_PATH=os.path.join(work, 'tracker.db'))
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING,
                        format='%(message)s')

    from crawler_core import downloader as D
    from crawler_core.db_manager import DBManager
    from crawler_core.pipeline import VideoPipeline, load_classifier

    if args.real_db:
        real = os.path.join(ROOT, 'data', 'db', 'tracker.db')
        shutil.copy2(real, os.path.join(work, 'tracker.db'))

    # URL giả dạng https (pipeline chặn URL không tuyệt đối) — mỗi file 1 URL riêng
    by_url = {f'https://offline.test/{i}/{os.path.basename(p)}': p for i, p in enumerate(files)}

    class LocalFileDownloader:
        """Thay yt-dlp: 'tải' = chép file có sẵn vào staging, đúng hợp đồng fetch()."""
        output_base = D.OUTPUT_BASE

        def fetch(self, url):
            src = by_url[url]
            staging = tempfile.mkdtemp(prefix='dl_', dir=D.ensure_staging_dir())
            dst = os.path.join(staging, f'{args.platform}_{os.path.basename(src)}')
            shutil.copy(src, dst)
            return dst

    event_filter = None
    if not args.no_videomae:
        from crawler_core.videomae_filter import VideoMAEEventFilter
        event_filter = VideoMAEEventFilter()

    pipe = VideoPipeline(
        DBManager(), LocalFileDownloader(),
        classifier=None if args.no_cctv else load_classifier(),
        platform=args.platform, category=args.category,
        skip_portrait=not args.no_portrait, dedup_content=not args.no_dedup,
        event_filter=event_filter,
    )

    print(f'\n{len(files)} video → {work}\n')
    for url, path in by_url.items():
        outcome = pipe.process(url, source_page='offline-test', category=args.category)
        truth = ''
        side = os.path.splitext(path)[0] + '.json'
        try:
            segs = json.load(open(side)).get('segments') or []
            truth = f"  | nhãn tay: {sorted({s['label'] for s in segs})}" if segs else ''
        except Exception:
            pass
        print(f'{outcome:16} {os.path.basename(path)}{truth}')

    print('\n' + pipe.stats.summary())
    con = sqlite3.connect(os.path.join(work, 'tracker.db'))
    print('\nChi tiết từng bộ lọc (filter_detail):')
    for url, fd in con.execute('SELECT url, filter_detail FROM video_urls WHERE url LIKE "https://offline.test/%"'):
        d = json.loads(fd or '{}')
        cl, vm = d.get('classifier') or {}, d.get('videomae') or {}
        print(f"  {os.path.basename(by_url.get(url, url))}: "
              f"CLIP={cl.get('label')}/{cl.get('max_prob')}  "
              f"VideoMAE={vm.get('is_event')} {(vm.get('best') or {}).get('label')}/"
              f"{(vm.get('best') or {}).get('prob')}")
    print(f'\nXem video nhận / bị loại: {work}   (xóa thư mục khi xong)')


if __name__ == '__main__':
    main()
