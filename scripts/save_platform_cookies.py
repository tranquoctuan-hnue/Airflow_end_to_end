#!/usr/bin/env python3
"""
Mở browser, đăng nhập thủ công, lưu cookie cho platform bất kỳ.

⚠ THƯỜNG NÊN DÙNG scripts/import_browser_cookies.py THAY CHO SCRIPT NÀY.

Script này thất bại với mọi platform đăng nhập qua "Continue with Google"
(X, TikTok...): Google phát hiện browser bị điều khiển tự động và chặn với
    "Couldn't sign you in — This browser or app may not be secure"
Đã kiểm chứng thực tế; kể cả dùng Chrome thật + cookie hợp lệ vẫn bị chặn.

Cách chắc chắn: đăng nhập bằng tay trong Chrome thường của bạn (như người
dùng bình thường), rồi nhập cookie sang crawler:
    python scripts/import_browser_cookies.py --list
    python scripts/import_browser_cookies.py tiktok x instagram

Script này chỉ còn hữu ích khi platform cho đăng nhập bằng email/mật khẩu
trực tiếp và bạn không muốn dùng profile Chrome cá nhân.

Cùng cách làm như save_fb_cookies.py nhưng dùng được cho mọi platform, và
lưu ĐỒNG THỜI 2 định dạng vì hai công cụ đọc khác nhau:
    cookies/<platform>_cookies.json  → Playwright (discovery)
    cookies/<platform>_cookies.txt   → yt-dlp (download)

    python scripts/save_platform_cookies.py instagram
    python scripts/save_platform_cookies.py x
    python scripts/save_platform_cookies.py tiktok
    python scripts/save_platform_cookies.py reddit
    python scripts/save_platform_cookies.py --list

Browser KHÔNG tự đóng khi lỗi — luôn in lý do rồi chờ bạn bấm Enter, để
không bị mất thông tin lỗi.

LƯU Ý VỚI REDDIT: cookie chỉ dùng cho fallback Playwright. Cách ổn định hơn
nhiều là OAuth (token 24h, không bị bot-detection) — xem hướng dẫn ở cuối
khi chạy script này với platform reddit.
"""

import argparse
import json
import os
import sys

REPO_ROOT   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COOKIES_DIR = os.path.join(REPO_ROOT, 'cookies')

# platform → (URL đăng nhập, URL kiểm tra sau khi login, [cookie xác nhận đã login], [domain lấy cookie])
PLATFORMS = {
    'instagram': (
        'https://www.instagram.com/accounts/login/',
        'https://www.instagram.com/',
        ['sessionid', 'ds_user_id'],
        ['https://www.instagram.com', 'https://.instagram.com'],
    ),
    'x': (
        'https://x.com/i/flow/login',
        'https://x.com/home',
        ['auth_token', 'ct0'],
        ['https://x.com', 'https://.x.com', 'https://twitter.com', 'https://.twitter.com'],
    ),
    'tiktok': (
        'https://www.tiktok.com/login',
        'https://www.tiktok.com/',
        ['sessionid', 'sid_tt'],
        ['https://www.tiktok.com', 'https://.tiktok.com'],
    ),
    'reddit': (
        'https://www.reddit.com/login',
        'https://www.reddit.com/',
        ['reddit_session', 'token_v2'],
        ['https://www.reddit.com', 'https://.reddit.com'],
    ),
    'youtube': (
        'https://accounts.google.com/ServiceLogin?service=youtube',
        'https://www.youtube.com/',
        ['SAPISID', 'SID', '__Secure-1PSID'],
        ['https://www.youtube.com', 'https://.youtube.com',
         'https://.google.com'],
    ),
    'vimeo': (
        'https://vimeo.com/log_in',
        'https://vimeo.com/',
        ['vimeo', 'vuid'],
        ['https://vimeo.com', 'https://.vimeo.com'],
    ),
    'dailymotion': (
        'https://www.dailymotion.com/signin',
        'https://www.dailymotion.com/',
        ['access_token', 'v1st', 'dmvk'],
        ['https://www.dailymotion.com', 'https://.dailymotion.com'],
    ),
}


def save_netscape(cookies: list, path: str):
    """Định dạng Netscape cho yt-dlp (giống save_fb_cookies.py)."""
    lines = ['# Netscape HTTP Cookie File']
    for c in cookies:
        domain  = c.get('domain', '')
        inc_sub = 'TRUE' if domain.startswith('.') else 'FALSE'
        cpath   = c.get('path', '/')
        secure  = 'TRUE' if c.get('secure', False) else 'FALSE'
        exp     = int(c.get('expires', 0) or 0)
        if exp < 0:
            exp = 0
        lines.append(
            f"{domain}\t{inc_sub}\t{cpath}\t{secure}\t{exp}\t{c['name']}\t{c['value']}"
        )
    with open(path, 'w') as f:
        f.write('\n'.join(lines) + '\n')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('platform', nargs='?', help=f"một trong: {', '.join(PLATFORMS)}")
    ap.add_argument('--list', action='store_true', help='liệt kê platform hỗ trợ')
    args = ap.parse_args()

    if args.list or not args.platform:
        print('Platform hỗ trợ:')
        for name, (login, _, verify, _d) in PLATFORMS.items():
            have = os.path.exists(os.path.join(COOKIES_DIR, f'{name}_cookies.txt'))
            print(f"  {name:12} {'✓ đã có cookie' if have else '— chưa có':18} {login}")
        return 0

    platform = args.platform.lower()
    if platform not in PLATFORMS:
        print(f"Không hỗ trợ '{platform}'. Có: {', '.join(PLATFORMS)}")
        return 1

    login_url, check_url, verify_names, domains = PLATFORMS[platform]
    os.makedirs(COOKIES_DIR, exist_ok=True)
    json_path = os.path.join(COOKIES_DIR, f'{platform}_cookies.json')
    txt_path  = os.path.join(COOKIES_DIR, f'{platform}_cookies.txt')

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print('playwright chưa được cài: pip install playwright && playwright install chromium')
        return 1

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False, args=['--start-maximized'])
        ctx = browser.new_context(
            user_agent=(
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
            ),
            no_viewport=True,
        )
        page = ctx.new_page()
        try:
            page.goto(login_url, wait_until='domcontentloaded', timeout=60_000)
        except Exception as e:
            print(f'[WARN] Không mở được {login_url}: {e}')

        print('\n' + '=' * 62)
        print(f'Browser đã mở — hãy đăng nhập {platform.upper()}.')
        print('Có 2FA / captcha / xác minh email thì hoàn thành luôn.')
        if platform == 'reddit':
            print('Reddit: nhớ bật "I am over 18" trong Settings để thấy bài NSFW.')
        print('Sau khi vào được trang chủ → quay lại đây bấm Enter.')
        print('=' * 62 + '\n')
        input('>>> Bấm Enter sau khi đã đăng nhập xong: ')

        # Navigate lại trang chủ để chắc chắn session cookie được set đầy đủ
        try:
            page.goto(check_url, wait_until='domcontentloaded', timeout=30_000)
            page.wait_for_timeout(2_500)
        except Exception:
            pass

        try:
            cookies = ctx.cookies(domains)
        except Exception:
            cookies = ctx.cookies()

        found = {c['name'] for c in cookies}
        hits  = [n for n in verify_names if n in found]

        print(f'\nTìm thấy {len(cookies)} cookie.')
        print(f'Cookie xác nhận đăng nhập ({"/".join(verify_names)}): '
              f'{", ".join(hits) if hits else "KHÔNG CÓ"}')

        if not hits:
            print('\n[LỖI] Chưa thấy cookie đăng nhập — có thể chưa login xong,')
            print('      hoặc bị checkpoint/captcha. Kiểm tra lại trong browser.')
            print('      KHÔNG lưu file để tránh ghi đè cookie cũ đang dùng được.')
            input('\n>>> Bấm Enter để đóng browser: ')
            browser.close()
            return 1

        try:
            with open(json_path, 'w') as f:
                json.dump(cookies, f, indent=2)
            save_netscape(cookies, txt_path)
        except Exception as e:
            print(f'\n[LỖI] Ghi file thất bại: {e}')
            input('\n>>> Bấm Enter để đóng browser: ')
            browser.close()
            return 1

        print(f'\n[OK] Đã lưu {len(cookies)} cookie:')
        print(f'  Playwright : {json_path}')
        print(f'  yt-dlp     : {txt_path}')

        if platform == 'reddit':
            print('\n' + '-' * 62)
            print('Reddit: cookie chỉ dùng cho fallback Playwright và dễ hết hạn.')
            print('Cách ổn định hơn nhiều (token 24h, không bị bot-detect):')
            print('  1. Vào https://www.reddit.com/prefs/apps → "create app"')
            print('  2. Chọn type "script", redirect uri: http://localhost:8080')
            print('  3. Thêm vào file .env:')
            print('       REDDIT_CLIENT_ID=<id ngay dưới tên app>')
            print('       REDDIT_CLIENT_SECRET=<dòng secret>')
            print('       REDDIT_USERNAME=<nick của bạn>')
            print('       REDDIT_PASSWORD=<mật khẩu>      # có 2FA: "matkhau:OTP"')
            print('  → Có USERNAME/PASSWORD mới thấy được bài NSFW.')
            print('-' * 62)

        input('\n>>> Bấm Enter để đóng browser: ')
        browser.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
