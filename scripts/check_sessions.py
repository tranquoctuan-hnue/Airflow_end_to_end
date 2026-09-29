"""
Kiểm tra phiên đăng nhập của crawler; phiên chết thì tự lấy lại cookie từ Chrome.
DAG crawler tự làm việc này đầu mỗi lượt — script dùng để xem nhanh / làm ngay.

    airflow_venv/bin/python scripts/check_sessions.py               # mọi platform
    airflow_venv/bin/python scripts/check_sessions.py x facebook    # vài platform
    airflow_venv/bin/python scripts/check_sessions.py --check       # chỉ kiểm tra, không lấy lại
    airflow_venv/bin/python scripts/check_sessions.py --notify-test # thử gửi thông báo

Báo "cần đăng nhập" → mở Chrome, đăng nhập platform đó như bình thường, rồi chạy lại
script (hoặc cứ để đó — lượt crawl sau tự lấy).
"""

import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

# Chạy tay ngoài Airflow: nạp .env (COOKIE_PROFILE, TELEGRAM_*…) như start_airflow.sh làm
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(ROOT, '.env'))
except ImportError:
    pass

from platform_crawlers import sessions  # noqa: E402

ICON = {'alive': '✓', 'refreshed': '✓', 'login_needed': '✗', 'disabled': '–', 'unknown': '?'}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('platforms', nargs='*', help=f'mặc định: {" ".join(sessions.PLATFORMS)}')
    ap.add_argument('--check', action='store_true', help='chỉ kiểm tra, không lấy lại từ Chrome')
    ap.add_argument('--notify-test', action='store_true', help='gửi thử 1 thông báo desktop/Telegram')
    args = ap.parse_args()

    if args.notify_test:
        res = sessions.notify('Thử thông báo crawler', 'Thấy tin này là kênh thông báo hoạt động.')
        for ch, r in res.items():
            print(f"  {'✓' if r == 'ok' else '✗'} {ch:9} {r}")
        if res.get('telegram', '').startswith('lỗi 404'):
            print('    → token sai: phải là cả chuỗi "<số>:<35 ký tự>" BotFather đưa (gửi /token cho @BotFather)')
        elif 'chat not found' in res.get('telegram', ''):
            print('    → chat id sai, hoặc chưa nhắn cho bot tin nào (mở bot, bấm Start, rồi lấy lại chat id)')
        return 0 if res.get('telegram') == 'ok' else 1

    bad = [p for p in args.platforms if p not in sessions.PLATFORMS]
    if bad:
        sys.exit(f'Không hỗ trợ: {bad}. Chọn trong: {", ".join(sessions.PLATFORMS)}')

    worst = 0
    for p in args.platforms or sessions.PLATFORMS:
        print(f'→ {p} …', flush=True)
        r = sessions.ensure_session(p, check_only=args.check, notify_user=False)
        print(f'  {ICON[r.state]} {p:12} {r.state:12} {r.message}')
        worst |= r.state == 'login_needed'
    return worst


if __name__ == '__main__':
    sys.exit(main())
