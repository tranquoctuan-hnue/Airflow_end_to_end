#!/usr/bin/env python3
"""
Backfill fingerprint cho video ĐÃ TẢI trước khi có hệ thống dedup.

Vì sao cần: bảng video_fingerprints mới được thêm nên video tải trước đó
chưa có fingerprint. Không backfill thì dedup bắt đầu từ số 0 — clip đã có
sẵn trên đĩa sẽ bị tải lại từ platform khác mà không bị chặn.

HAI CHẾ ĐỘ
──────────
  --scan-dir DIR   Quét mọi file video trong DIR (KHUYẾN NGHỊ cho repo này,
                   vì file_path trong DB đã lỗi thời — file thật nằm ở
                   data/videos/check/). Tên file dạng <id>.mp4 được dùng để
                   truy lại URL gốc trong DB → phục hồi cả canonical ID (L1).

  (mặc định)       Dùng file_path ghi trong bảng video_urls.

Mặc định DRY-RUN: tính fingerprint, ghi vào DB, BÁO CÁO file trùng nhưng
KHÔNG xóa gì. Thêm --delete-duplicates mới thực sự xóa.

    # Bước 1 — thử 20 file để xem tốc độ và kết quả
    python scripts/backfill_fingerprints.py --scan-dir data/videos/check --limit 20

    # Bước 2 — chạy toàn bộ (2300+ file, mất khoảng 1-1.5 giờ)
    python scripts/backfill_fingerprints.py --scan-dir data/videos/check

    # Bước 3 — CHỈ chạy sau khi đã xem báo cáo và đồng ý xóa file trùng
    python scripts/backfill_fingerprints.py --scan-dir data/videos/check --delete-duplicates

Script có thể chạy lại nhiều lần: file đã có fingerprint sẽ được bỏ qua,
nên bị ngắt giữa đường vẫn tiếp tục được.
"""

import argparse
import os
import sqlite3
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from crawler_core import dedup                      # noqa: E402
from crawler_core.db_manager import DBManager       # noqa: E402

VIDEO_EXTS = ('.mp4', '.mkv', '.webm', '.mov', '.avi', '.ts')


# ── Thu thập danh sách cần xử lý ─────────────────────────────────────────────

def targets_from_db(db: DBManager, limit: int | None) -> list[tuple[str, str]]:
    """(url, file_path) của video downloaded chưa có fingerprint."""
    sql = '''
        SELECT u.url, u.file_path
        FROM video_urls u
        WHERE u.status = 'downloaded'
          AND u.file_path IS NOT NULL AND u.file_path != ''
          AND NOT EXISTS (
              SELECT 1 FROM video_fingerprints f WHERE f.file_path = u.file_path
          )
        ORDER BY u.id
    '''
    if limit:
        sql += f' LIMIT {int(limit)}'
    with sqlite3.connect(db.db_path) as conn:
        return conn.execute(sql).fetchall()


def targets_from_dir(db: DBManager, root: str, limit: int | None) -> list[tuple[str | None, str]]:
    """
    (url|None, file_path) cho mọi file video trong root chưa có fingerprint.

    url được truy lại từ DB bằng tên file: file_path cũ trong DB có dạng
    .../CCTV/<id>.mp4 nên khớp theo stem là đủ để phục hồi URL gốc → có
    canonical ID (L1) chính xác thay vì đoán.
    """
    with sqlite3.connect(db.db_path) as conn:
        done = {
            row[0] for row in conn.execute(
                'SELECT file_path FROM video_fingerprints WHERE file_path IS NOT NULL'
            )
        }
        # stem → url, dựng 1 lần để không query từng file
        stem_to_url: dict[str, str] = {}
        for url, fp in conn.execute(
            'SELECT url, file_path FROM video_urls '
            'WHERE file_path IS NOT NULL AND file_path != ""'
        ):
            stem_to_url.setdefault(os.path.splitext(os.path.basename(fp))[0], url)

    out: list[tuple[str | None, str]] = []
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in sorted(filenames):
            if not name.lower().endswith(VIDEO_EXTS):
                continue
            path = os.path.join(dirpath, name)
            if path in done:
                continue
            out.append((stem_to_url.get(os.path.splitext(name)[0]), path))
            if limit and len(out) >= limit:
                return out
    return out


def guess_canonical(url: str | None, file_path: str):
    """
    Canonical ID từ URL nếu có. Nếu không, tên file toàn số → Facebook
    video ID (toàn bộ dữ liệu cũ của repo này đến từ Facebook).
    """
    if url:
        c = dedup.extract_canonical_id(url)
        if c:
            return c
    stem = os.path.splitext(os.path.basename(file_path))[0]
    if stem.isdigit() and len(stem) >= 8:
        return ('facebook', 'video', stem)
    return None


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--scan-dir', default=None,
                    help='quét thư mục thay vì dùng file_path trong DB')
    ap.add_argument('--limit', type=int, default=None,
                    help='chỉ xử lý N file đầu (để thử)')
    ap.add_argument('--delete-duplicates', action='store_true',
                    help='XÓA file trùng (mặc định chỉ báo cáo)')
    args = ap.parse_args()

    db = DBManager()

    if args.scan_dir:
        root = os.path.abspath(args.scan_dir)
        if not os.path.isdir(root):
            print(f"Không phải thư mục: {root}")
            return 1
        rows = targets_from_dir(db, root, args.limit)
        mode = f'quét thư mục {root}'
    else:
        rows = targets_from_db(db, args.limit)
        mode = 'file_path trong DB'

    print(f"DB              : {db.db_path}")
    print(f"Nguồn           : {mode}")
    print(f"Fingerprint có  : {db.fingerprint_count()}")
    print(f"Cần xử lý       : {len(rows)}")
    print(f"Chế độ          : "
          f"{'XÓA file trùng' if args.delete_duplicates else 'DRY-RUN (chỉ báo cáo)'}")
    print('-' * 72)

    if not rows:
        print("Không có gì để backfill.")
        return 0

    added = missing = failed = 0
    duplicates: list[tuple[str, str, str]] = []
    t0 = time.monotonic()

    for i, (url, file_path) in enumerate(rows, 1):
        if not os.path.exists(file_path):
            missing += 1
            continue

        canonical = guess_canonical(url, file_path)
        file_hash = dedup.compute_file_hash(file_path)
        duration  = dedup.get_video_duration(file_path)
        thumb     = dedup.compute_thumbnail_hash(file_path, duration)
        fp        = dedup.compute_fingerprint(file_path, duration)

        if not file_hash and not thumb and not fp:
            failed += 1
            print(f"  [{i}/{len(rows)}] LỖI không tính được hash: {file_path}")
            continue

        # Trùng với thứ đã có trong store chưa?
        hit_layer = hit_orig = None
        if canonical and (o := db.find_by_canonical_id(*canonical)):
            hit_layer, hit_orig = 'L1/canonical', o
        elif file_hash and (o := db.find_by_file_hash(file_hash)):
            hit_layer, hit_orig = 'L2/file-hash', o
        elif thumb and duration > 0 and (o := db.find_by_thumbnail(thumb, duration)):
            hit_layer, hit_orig = 'L3/thumbnail', o
        elif fp and duration > 0 and (o := db.find_by_fingerprint(fp, duration)):
            hit_layer, hit_orig = 'L4/fingerprint', o

        if hit_layer:
            duplicates.append((hit_layer, file_path, hit_orig or '?'))
            print(f"  [{i}/{len(rows)}] TRÙNG {hit_layer}: {os.path.basename(file_path)}")
            print(f"                 ↳ giống: {hit_orig}")
            if args.delete_duplicates:
                try:
                    os.remove(file_path)
                    if url:
                        db.update_url_status(url, 'skipped_duplicate')
                    print("                 ↳ đã xóa file")
                except Exception as e:
                    print(f"                 ↳ xóa thất bại: {e}")
            continue

        p, otype, oid = canonical if canonical else (None, None, None)
        db.save_video_fingerprint(
            url=url or file_path, file_path=file_path, duration=duration,
            source_platform=p, canonical_object_type=otype, canonical_object_id=oid,
            file_hash=file_hash, thumbnail_hash=thumb, fingerprint=fp,
        )
        added += 1

        if i % 25 == 0 or i == len(rows):
            elapsed = time.monotonic() - t0
            rate    = i / elapsed if elapsed else 0
            eta     = (len(rows) - i) / rate if rate else 0
            print(f"  [{i}/{len(rows)}] +{added} fingerprint, "
                  f"{len(duplicates)} trùng | {rate:.1f} file/s, "
                  f"còn ~{eta/60:.0f} phút")

    print('-' * 72)
    print(f"Fingerprint thêm mới : {added}")
    print(f"File trùng phát hiện : {len(duplicates)}"
          f"{' (ĐÃ XÓA)' if args.delete_duplicates else ' (CHƯA xóa)'}")
    print(f"File không tồn tại   : {missing}")
    print(f"Không tính được hash : {failed}")
    print(f"Tổng fingerprint     : {db.fingerprint_count()}")
    print(f"Thời gian            : {(time.monotonic()-t0)/60:.1f} phút")

    if duplicates and not args.delete_duplicates:
        print()
        print("Xem lại danh sách trên. Nếu đồng ý xóa các file trùng, chạy lại với:")
        cmd = 'python scripts/backfill_fingerprints.py'
        if args.scan_dir:
            cmd += f' --scan-dir {args.scan_dir}'
        print(f"  {cmd} --delete-duplicates")
    return 0


if __name__ == '__main__':
    sys.exit(main())
