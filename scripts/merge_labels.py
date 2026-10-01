"""
Gộp nhãn cũ vào nhãn mới — cả DB lẫn file trên đĩa/NAS.

Mặc định CHẠY THỬ (chỉ in ra sẽ làm gì). Thêm --apply để làm thật; trước khi sửa
sẽ tự sao lưu tracker.db thành tracker.db.bak-<thời điểm>.

    python scripts/merge_labels.py                      # chạy thử, cặp mặc định
    python scripts/merge_labels.py --apply
    python scripts/merge_labels.py --map Shoplifting=Stealing --apply

Làm gì với mỗi dòng video_urls có final_category = nhãn cũ:
  - Đổi final_category sang nhãn mới (đây là thứ crawler dùng để đếm video mỗi
    nhãn — platform_crawlers/label_cursor sắp nhãn thiếu nhất lên trước).
  - Nếu file còn tồn tại và nằm trong thư mục <...>/<nhãn cũ>/ → chuyển sang
    <...>/<nhãn mới>/ cùng cấp (áp dụng cho cả dataset lẫn video_rejected), cập
    nhật file_path ở video_urls + video_fingerprints, sửa "category" trong file
    .json đi kèm (video bị loại).
  - File không còn ở đường dẫn trong DB (dataset đã chuyển đi nơi khác) → chỉ
    đổi nhãn trong DB, giữ nguyên file_path.
Ngoài ra file trong <OUTPUT_BASE>/<nhãn cũ>/ không có trong DB cũng được chuyển.
"""

import argparse
import json
import os
import shutil
import sqlite3
import sys
from collections import Counter
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from crawler_core.db_manager import DB_PATH
from crawler_core.downloader import OUTPUT_BASE, move_file

DEFAULT_MAP = {'Shoplifting': 'Stealing', 'FallOver': 'Collapse'}


def _target_path(path: str, old: str, new: str) -> str | None:
    """<...>/<old>/file.mp4 → <...>/<new>/file.mp4, None nếu không nằm trong <old>/."""
    parent = os.path.dirname(path)
    if os.path.basename(parent) != old:
        return None
    return os.path.join(os.path.dirname(parent), new, os.path.basename(path))


def _fix_sidecar(video_path: str, new: str, apply: bool):
    sidecar = os.path.splitext(video_path)[0] + '.json'
    if not os.path.exists(sidecar):
        return
    if apply:
        with open(sidecar, encoding='utf-8') as f:
            info = json.load(f)
        if info.get('type') == 'video_classification':
            return      # file annotation (crawler_core/annotation.py): không có nhãn crawl
        info['category'] = new
        with open(sidecar, 'w', encoding='utf-8') as f:
            json.dump(info, f, ensure_ascii=False, indent=2)


def merge(old: str, new: str, conn, apply: bool) -> Counter:
    c = Counter()
    rows = conn.execute(
        'SELECT url, file_path FROM video_urls WHERE final_category = ?', (old,)
    ).fetchall()
    c['dòng DB'] = len(rows)

    for url, path in rows:
        dest = _target_path(path, old, new) if path else None
        if not (path and dest and os.path.exists(path)):
            c['chỉ đổi nhãn (không có file ở đường dẫn cũ)'] += 1
            if apply:
                conn.execute('UPDATE video_urls SET final_category = ? WHERE url = ?',
                             (new, url))
            continue
        if os.path.exists(dest):
            c['BỎ QUA — file đích đã tồn tại'] += 1
            print(f'  ! trùng tên, bỏ qua: {dest}')
            continue
        c['chuyển file + đổi nhãn'] += 1
        if not apply:
            continue
        moved = move_file(path, os.path.dirname(dest))
        if not moved:
            c['LỖI chuyển file'] += 1
            continue
        for ext in ('.json',):            # file đi kèm video bị loại
            side = os.path.splitext(path)[0] + ext
            if os.path.exists(side):
                move_file(side, os.path.dirname(dest))
        _fix_sidecar(moved, new, apply)
        conn.execute(
            'UPDATE video_urls SET final_category = ?, file_path = ? WHERE url = ?',
            (new, moved, url))
        conn.execute('UPDATE video_fingerprints SET file_path = ? WHERE file_path = ?',
                     (moved, path))

    # File trong dataset mà DB không biết tới
    old_dir = os.path.join(OUTPUT_BASE, old)
    if os.path.isdir(old_dir):
        known = {p for (p,) in conn.execute(
            'SELECT file_path FROM video_urls WHERE file_path LIKE ?', (old_dir + '%',))}
        for name in sorted(os.listdir(old_dir)):
            src = os.path.join(old_dir, name)
            if src in known or not os.path.isfile(src):
                continue
            c['file ngoài DB được chuyển'] += 1
            if apply:
                move_file(src, os.path.join(OUTPUT_BASE, new))
        if apply and not os.listdir(old_dir):
            os.rmdir(old_dir)
            c['xóa thư mục rỗng'] += 1
    return c


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--map', action='append', metavar='CŨ=MỚI',
                    help=f'cặp nhãn cần gộp (mặc định: {DEFAULT_MAP})')
    ap.add_argument('--apply', action='store_true', help='làm thật (mặc định chạy thử)')
    args = ap.parse_args()

    mapping = dict(m.split('=', 1) for m in args.map) if args.map else DEFAULT_MAP
    print(f"{'THỰC HIỆN' if args.apply else 'CHẠY THỬ (thêm --apply để làm thật)'}")
    print(f'DB     : {DB_PATH}\nDataset: {OUTPUT_BASE}\n')

    if args.apply:
        backup = f"{DB_PATH}.bak-{datetime.now():%Y%m%d-%H%M%S}"
        shutil.copy2(DB_PATH, backup)
        print(f'Đã sao lưu DB → {backup}\n')

    with sqlite3.connect(DB_PATH) as conn:
        for old, new in mapping.items():
            print(f'{old} → {new}')
            for k, v in merge(old, new, conn, args.apply).items():
                print(f'  {k:45} {v}')
        if args.apply:
            conn.commit()


if __name__ == '__main__':
    main()
