"""
Tách tài khoản camera ra khỏi rtsp/*.txt → Airflow Connection (mã hóa trong airflow.db).

    airflow_venv/bin/python scripts/migrate_rtsp_credentials.py              # CHẠY THỬ, không sửa gì
    airflow_venv/bin/python scripts/migrate_rtsp_credentials.py --apply      # làm thật
    airflow_venv/bin/python scripts/migrate_rtsp_credentials.py --redact-logs          # xem log nào lộ mật khẩu
    airflow_venv/bin/python scripts/migrate_rtsp_credentials.py --redact-logs --apply  # che mật khẩu trong log cũ

--apply:
  1. Mỗi cặp user:password khác nhau → 1 Connection rtsp_cam_<n> (kiểu Generic).
  2. Sao lưu file gốc vào rtsp/.backup_<thời điểm>/ (quyền 700, không vào git) —
     XÓA thư mục này sau khi đã kiểm tra camera chạy được.
  3. Viết lại mỗi URL thành rtsp://{rtsp_cam_<n>}@host/... — chỉ ghi file khi đã
     kiểm tra URL ghép lại (src/modules/rtsp_credentials.resolve) GIỐNG HỆT bản gốc.
Không in mật khẩu ra màn hình. Chạy lại an toàn: URL đã ở dạng mới được bỏ qua.
"""

import argparse
import glob
import hashlib
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from src.modules import rtsp_credentials as R  # noqa: E402

_INLINE = re.compile(r'^(rtsps?://)([^/]*)@([^@/]+(?:/.*)?)$')


def is_url_list(path) -> bool:
    """Chỉ tự sửa file DANH SÁCH URL: mọi dòng là URL (rtsp/http/https — YouTube live
    cũng hợp lệ), dòng trống hoặc #chú thích. Có dòng chữ tự do → file ghi chú."""
    lines = [l.strip().strip('"').strip("'") for l in open(path, encoding='utf-8')]
    return all(not l or l.startswith('#') or re.match(r'^[a-z]+://', l) for l in lines)


def parse_files(rtsp_dir):
    """{file: [(dòng gốc, quote, url)]}, {(user, pass): conn_id}, [file ghi chú bỏ qua]"""
    files, pairs, notes = {}, {}, []
    for path in sorted(glob.glob(os.path.join(rtsp_dir, '*.txt'))):
        if path.endswith('.example.txt'):
            continue
        if not is_url_list(path):
            notes.append(path)
            continue
        rows = []
        for line in open(path, encoding='utf-8').read().splitlines():
            s = line.strip()
            quote = '"' if s.startswith('"') else ("'" if s.startswith("'") else '')
            url = s.strip('"').strip("'")
            rows.append((line, quote, url))
            m = _INLINE.match(url)
            if m and ':' in m.group(2) and not R._PLACEHOLDER.match(url):
                user, pwd = m.group(2).split(':', 1)
                pairs.setdefault((user, pwd), None)
        files[path] = rows
    for i, key in enumerate(pairs, start=1):
        pairs[key] = f'rtsp_cam_{i}'
    return files, pairs, notes


def rewrite(rows, pairs):
    out, changed = [], 0
    for line, quote, url in rows:
        m = _INLINE.match(url)
        if m and ':' in m.group(2) and not R._PLACEHOLDER.match(url):
            user, pwd = m.group(2).split(':', 1)
            new = f'{m.group(1)}{{{pairs[(user, pwd)]}}}@{m.group(3)}'
            out.append(f'{quote}{new}{quote}')
            changed += 1
        else:
            out.append(line)
    return out, changed


def verify(files, pairs, new_content):
    """Ghép lại bằng chính code chạy thật (resolve) và so với URL gốc."""
    saved = dict(os.environ)
    try:
        for (user, pwd), cid in pairs.items():
            os.environ[f'RTSP_CRED_{cid.upper()}'] = f'{user}:{pwd}'
        R._cache.clear()
        bad = 0
        for path, rows in files.items():
            for (_, _, orig), new_line in zip(rows, new_content[path]):
                new_url = new_line.strip().strip('"').strip("'")
                if new_url and R.resolve(new_url) != orig:
                    bad += 1
        return bad
    finally:
        os.environ.clear(); os.environ.update(saved); R._cache.clear()


def airflow(*args):
    env = {**os.environ, 'AIRFLOW_HOME': os.environ.get('AIRFLOW_HOME', ROOT)}
    exe = os.path.join(os.path.dirname(sys.executable), 'airflow')
    return subprocess.run([exe, *args], env=env, capture_output=True, text=True)


def redact_logs(apply: bool):
    log_dir = os.path.join(ROOT, 'logs')
    hit_files = hit_lines = 0
    for dirpath, _, names in os.walk(log_dir):
        for name in names:
            path = os.path.join(dirpath, name)
            try:
                with open(path, encoding='utf-8', errors='surrogateescape') as f:
                    if 'rtsp://' not in f.read(1 << 16) and os.path.getsize(path) < (1 << 16):
                        continue
            except OSError:
                continue
            n = 0
            fd, tmp = tempfile.mkstemp(dir=dirpath)
            with os.fdopen(fd, 'w', encoding='utf-8', errors='surrogateescape') as out, \
                 open(path, encoding='utf-8', errors='surrogateescape') as src:
                for line in src:
                    clean = R.redact_text(line)
                    n += clean != line
                    out.write(clean)
            if n and apply:
                shutil.copymode(path, tmp)
                os.replace(tmp, path)
            else:
                os.remove(tmp)
            if n:
                hit_files += 1
                hit_lines += n
    verb = 'Đã che' if apply else 'Sẽ che (thêm --apply)'
    print(f'{verb} mật khẩu RTSP ở {hit_lines} dòng trong {hit_files} file log dưới logs/')


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--apply', action='store_true', help='làm thật (mặc định chạy thử)')
    ap.add_argument('--redact-logs', action='store_true', help='xử lý log cũ thay vì file rtsp')
    ap.add_argument('--rtsp-dir', default=os.path.join(ROOT, 'rtsp'))
    ap.add_argument('--no-connections', action='store_true',
                    help='không tạo Airflow Connection (dùng env RTSP_CRED_* thay thế)')
    args = ap.parse_args()

    if args.redact_logs:
        return redact_logs(args.apply)

    files, pairs, notes = parse_files(args.rtsp_dir)
    for path in notes:
        n = sum(1 for l in open(path, encoding='utf-8') if '@' in l or 'admin/' in l)
        print(f'⚠  {os.path.basename(path)}: file GHI CHÚ (có dòng không phải URL) — KHÔNG tự sửa. '
              f'{n} dòng có thể chứa mật khẩu: chuyển ra nơi an toàn ngoài dự án (trình quản '
              f'lý mật khẩu) rồi xóa khỏi rtsp/, hoặc sửa tay thành danh sách URL.')
    new_content, total = {}, 0
    for path, rows in files.items():
        new_content[path], n = rewrite(rows, pairs)
        total += n
    if not total:
        print('Không còn URL nào có mật khẩu viết thẳng — không có gì để làm.')
        return
    bad = verify(files, pairs, new_content)

    print(f"{'THỰC HIỆN' if args.apply else 'CHẠY THỬ (thêm --apply để làm thật)'}\n")
    for (user, pwd), cid in pairs.items():
        used = [os.path.basename(p) for p, rows in files.items()
                if any(f'{user}:{pwd}@' in u for _, _, u in rows)]
        fp = hashlib.sha1(pwd.encode()).hexdigest()[:6]
        print(f'  {cid:12} login={user!r} mật khẩu=*** (dấu vân tay {fp})  ← {", ".join(used)}')
    print(f'\n  {total} URL trong {sum(1 for p in files if new_content[p] != [r[0] for r in files[p]])} file '
          f'sẽ chuyển sang dạng rtsp://{{rtsp_cam_N}}@host/...')
    print(f'  Kiểm tra ghép lại: {"✓ khớp 100% với bản gốc" if not bad else f"✗ {bad} URL KHÔNG khớp"}')
    if bad:
        sys.exit('Dừng — không sửa gì vì có URL ghép lại không khớp.')
    if not args.apply:
        return

    if not args.no_connections:
        for (user, pwd), cid in pairs.items():
            airflow('connections', 'delete', cid)
            r = airflow('connections', 'add', cid, '--conn-type', 'generic',
                        '--conn-login', user, '--conn-password', pwd,
                        '--conn-description', 'Tài khoản camera RTSP (scripts/migrate_rtsp_credentials.py)')
            if r.returncode != 0:
                sys.exit(f'✗ Không tạo được Connection {cid}: {r.stderr.strip()[-300:]} — chưa sửa file nào')
            print(f'  ✓ Connection {cid}')

    backup = os.path.join(args.rtsp_dir, f'.backup_{datetime.now():%Y%m%d-%H%M%S}')
    os.makedirs(backup, mode=0o700)
    for path in files:
        if new_content[path] != [r[0] for r in files[path]]:
            shutil.copy2(path, backup)
            with open(path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(new_content[path]) + '\n')
    print(f'\n✓ Xong. Bản gốc (còn mật khẩu) ở {backup} — XÓA sau khi kiểm tra camera chạy được.')
    print('  Sửa / đổi mật khẩu sau này: Admin → Connections → rtsp_cam_N (không cần sửa file).')


if __name__ == '__main__':
    main()
