#!/usr/bin/env python3
"""
Tìm video TRÙNG NỘI DUNG đã lỡ tải về, bằng chính fingerprint L4 trong DB.

    python scripts/find_duplicate_videos.py                  # chỉ báo cáo
    python scripts/find_duplicate_videos.py --tolerance 0.3  # nới rào rộng hơn
    python scripts/find_duplicate_videos.py --delete-files    # xóa file thừa

VÌ SAO CẦN: rào `DURATION_TOLERANCE` từng đặt ±5%, quá hẹp so với các bản
repost bị cắt khác nhau, nên L3/L4 không hề được chạy cho những cặp lệch >5%
độ dài (xem ghi chú dài trong crawler_core/dedup.py). Số video trùng lọt qua
trong giai đoạn đó vẫn còn nguyên trên đĩa — script này dọn hậu quả đó.
Đã nới mặc định lên ±20%, nên từ giờ cặp mới sẽ bị chặn ngay khi tải.

⚠ SCRIPT NÀY KHÔNG BAO GIỜ XÓA DÒNG TRONG DB — kể cả với --delete-files.
`video_fingerprints` là toàn bộ ký ức dedup: xóa 1 dòng là video đó có thể bị
tải lại lần sau. Chỉ xóa FILE thừa trên đĩa, giữ nguyên mọi dòng DB.
(Ngày 2026-07-31 đã từng mất L1–L4 của 40 clip Facebook vì xóa dòng theo
source_platform — không hồi phục được.)

Cách chọn bản GIỮ LẠI trong mỗi nhóm: file lớn nhất còn tồn tại trên đĩa
(bitrate/độ phân giải cao nhất). File không đọc được (NAS chưa mount, đã bị
move đi) thì KHÔNG bao giờ bị chọn xóa và nhóm đó bị bỏ qua.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

DB_PATH = os.environ.get(
    'CRAWLER_DB_PATH',
    os.path.join(REPO_ROOT, 'data', 'db', 'tracker.db'),
)


def _fp_ints(fp: str) -> list[int] | None:
    """'aaa:bbb:...' → [int, ...]. popcount(XOR) đã kiểm chứng cho kết quả
    y hệt imagehash (2000/2000 cặp khớp), nhanh hơn nhiều lần."""
    try:
        return [int(x, 16) for x in fp.split(':') if x]
    except (ValueError, AttributeError):
        return None


def _avg_dist(a: list[int], b: list[int]) -> float:
    n = min(len(a), len(b))
    if not n:
        return 64.0
    return sum((a[i] ^ b[i]).bit_count() for i in range(n)) / n


def _fmt_size(nbytes: float) -> str:
    x = float(nbytes)
    for unit in ('B', 'KB', 'MB', 'GB', 'TB'):
        if x < 1024 or unit == 'TB':
            return f'{x:.1f} {unit}'
        x /= 1024
    return f'{x:.1f} TB'


def main() -> int:
    from crawler_core import dedup as D

    ap = argparse.ArgumentParser(description='Tìm video trùng nội dung đã tải')
    ap.add_argument('--tolerance', type=float, default=D.DURATION_TOLERANCE,
                    help=f'cửa sổ duration (mặc định {D.DURATION_TOLERANCE})')
    ap.add_argument('--threshold', type=float, default=D.FINGERPRINT_THRESHOLD,
                    help=f'ngưỡng L4 (mặc định {D.FINGERPRINT_THRESHOLD})')
    ap.add_argument('--delete-files', action='store_true',
                    help='XÓA file thừa trên đĩa (giữ nguyên mọi dòng DB)')
    args = ap.parse_args()

    conn = sqlite3.connect(f'file:{DB_PATH}?mode=ro', uri=True)
    rows = conn.execute(
        'SELECT url, duration, fingerprint, file_path FROM video_fingerprints '
        'WHERE fingerprint IS NOT NULL AND duration > 0 ORDER BY duration'
    ).fetchall()
    conn.close()

    items = []
    for url, dur, fp, path in rows:
        ints = _fp_ints(fp)
        if ints:
            items.append((float(dur), url, ints, path))
    print(f'Quét {len(items)} fingerprint, cửa sổ ±{args.tolerance:.0%}, '
          f'ngưỡng L4 < {args.threshold}', flush=True)

    # Union-find gộp các cặp trùng thành nhóm (A≈B, B≈C ⇒ cùng 1 nhóm)
    parent = list(range(len(items)))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    # items đã sort theo duration → chỉ cần quét tiến tới khi vượt cửa sổ.
    t0, pairs = time.perf_counter(), 0
    for i in range(len(items)):
        di, _u, fi, _p = items[i]
        hi = di * (1 + args.tolerance)
        for j in range(i + 1, len(items)):
            if items[j][0] > hi:
                break
            if _avg_dist(fi, items[j][2]) < args.threshold:
                union(i, j)
                pairs += 1
        if i and i % 2000 == 0:
            print(f'  ... {i}/{len(items)} ({time.perf_counter()-t0:.0f}s)', flush=True)

    groups: dict[int, list[int]] = {}
    for i in range(len(items)):
        groups.setdefault(find(i), []).append(i)
    groups = {k: v for k, v in groups.items() if len(v) > 1}

    print(f'\n{pairs} cặp trùng → {len(groups)} nhóm, '
          f'quét hết {time.perf_counter()-t0:.0f}s\n')
    if not groups:
        print('Không có nhóm trùng nào.')
        return 0

    total_waste, deleted, missing = 0, 0, 0
    for gi, (_root, idxs) in enumerate(sorted(groups.items(), key=lambda kv: -len(kv[1])), 1):
        sized = []
        for i in idxs:
            p = items[i][3]
            try:
                sized.append((os.path.getsize(p), i))
            except OSError:
                sized.append((-1, i))          # không đọc được → không bao giờ xóa
        sized.sort(reverse=True)
        keep_size, keep_i = sized[0]
        if keep_size < 0:
            # Cả nhóm không đọc được file → không thể chọn bản giữ lại. KHÔNG
            # im lặng bỏ qua: in ra để người vận hành biết có trùng, chỉ là
            # file_path trong DB đã cũ (video bị move sang thư mục khác, hoặc
            # NAS đổi mount point). Dedup vẫn hoạt động vì nó dựa trên DB.
            missing += 1
            print(f'[Nhóm {gi}] {len(idxs)} bản — TRÙNG nhưng không đọc được file '
                  f'(file_path trong DB đã cũ):')
            for i in idxs:
                print(f'   {items[i][0]:7.2f}s  {items[i][3] or "(không có path)"}')
            continue

        print(f'[Nhóm {gi}] {len(idxs)} bản')
        print(f'   GIỮ  {items[keep_i][0]:7.2f}s {_fmt_size(keep_size):>10}  '
              f'{os.path.basename(items[keep_i][3] or "?")}')
        for size, i in sized[1:]:
            if size < 0:
                print(f'   (bỏ qua, không đọc được file) {items[i][1][:60]}')
                continue
            total_waste += size
            print(f'   THỪA {items[i][0]:7.2f}s {_fmt_size(size):>10}  '
                  f'{os.path.basename(items[i][3] or "?")}')
            if args.delete_files:
                try:
                    os.remove(items[i][3])
                    deleted += 1
                except OSError as e:
                    print(f'        ! xóa hỏng: {e}')

    print(f'\nDung lượng thừa: {_fmt_size(total_waste)}')
    if missing:
        print(f'{missing} nhóm bị bỏ qua vì không đọc được file (NAS chưa mount?)')
    if args.delete_files:
        print(f'Đã xóa {deleted} file. Mọi dòng DB giữ nguyên — '
              f'các URL đó vẫn bị dedup chặn ở lần crawl sau.')
    else:
        print('Đây là chế độ BÁO CÁO. Thêm --delete-files để xóa file thừa.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
