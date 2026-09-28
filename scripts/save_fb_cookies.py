"""
Mở browser thủ công, đăng nhập Facebook, bấm Enter để lưu cookies.
Chạy: python scripts/save_fb_cookies.py  (cookie cho DAG Facebook nhóm cũ legacy_fb_groups_crawler;
DAG social_crawler_fb dùng scripts/import_browser_cookies.py facebook)
"""
import json, os
from playwright.sync_api import sync_playwright

_ROOT        = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COOKIES_FILE = os.path.join(_ROOT, 'cookies', 'facebook_legacy_cookies.json')
COOKIES_TXT  = os.path.join(_ROOT, 'cookies', 'facebook_legacy_cookies.txt')


def save_netscape(cookies, path):
    lines = ['# Netscape HTTP Cookie File']
    for c in cookies:
        domain  = c.get('domain', '')
        inc_sub = 'TRUE' if domain.startswith('.') else 'FALSE'
        cp      = c.get('path', '/')
        secure  = 'TRUE' if c.get('secure', False) else 'FALSE'
        exp     = int(c.get('expires', 0))
        if exp < 0: exp = 0
        lines.append(f"{domain}\t{inc_sub}\t{cp}\t{secure}\t{exp}\t{c['name']}\t{c['value']}")
    with open(path, 'w') as f:
        f.write('\n'.join(lines))


with sync_playwright() as pw:
    browser = pw.chromium.launch(
        headless=False,
        args=['--start-maximized'],
    )
    ctx = browser.new_context(
        user_agent=(
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
            '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
        ),
        no_viewport=True,
    )
    page = ctx.new_page()
    page.goto('https://www.facebook.com/login')

    print('\n' + '='*55)
    print('Browser đã mở. Hãy đăng nhập Facebook.')
    print('Nếu có xác minh 2FA / checkpoint → hoàn thành luôn.')
    print('Sau khi vào được trang chủ Facebook → bấm Enter ở đây.')
    print('='*55 + '\n')

    input('>>> Bấm Enter sau khi đã đăng nhập xong: ')

    # Navigate lại facebook.com để đảm bảo session cookie đầy đủ
    try:
        page.goto('https://www.facebook.com/', wait_until='domcontentloaded', timeout=15000)
        page.wait_for_timeout(2000)
    except Exception:
        pass

    cookies = ctx.cookies(['https://www.facebook.com', 'https://.facebook.com'])
    c_user  = next((c for c in cookies if c['name'] == 'c_user'), None)
    xs      = next((c for c in cookies if c['name'] == 'xs'), None)

    print(f'\nTìm thấy {len(cookies)} cookies:')
    for c in cookies:
        print(f"  {c['name']:30s} = {c['value'][:20]}...")

    if not c_user:
        print('\n[WARN] c_user không có — có thể chưa đăng nhập xong hoặc bị checkpoint.')
        print('Thử navigate lại...')
        try:
            page.goto('https://www.facebook.com/me', wait_until='domcontentloaded', timeout=15000)
            page.wait_for_timeout(3000)
            cookies = ctx.cookies(['https://www.facebook.com', 'https://.facebook.com'])
            c_user  = next((c for c in cookies if c['name'] == 'c_user'), None)
        except Exception:
            pass

    if c_user:
        os.makedirs(os.path.dirname(COOKIES_FILE), exist_ok=True)
        with open(COOKIES_FILE, 'w') as f:
            json.dump(cookies, f, indent=2)
        save_netscape(cookies, COOKIES_TXT)
        print(f'\n[OK] Saved {len(cookies)} cookies')
        print(f'  c_user = {c_user["value"]}')
        print(f'  xs     = {xs["value"][:10] if xs else "MISSING"}...')
        print(f'  File   : {COOKIES_FILE}')
    else:
        print('\n[ERROR] Không lưu — c_user vẫn thiếu.')
        print('Kiểm tra lại trong browser xem đã đăng nhập chưa.')

    browser.close()
