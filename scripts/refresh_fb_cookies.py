"""
Làm mới cookie đăng nhập Facebook.

Cách dùng:
    python scripts/refresh_fb_cookies.py

Một cửa sổ trình duyệt sẽ mở ra. Hãy ĐĂNG NHẬP Facebook thủ công
(nhập email/mật khẩu, qua 2FA/captcha nếu có). Khi đã vào được trang chủ,
script tự phát hiện cookie `c_user` và lưu vào:
    cookies/facebook_legacy_cookies.json   (cho Playwright)
    cookies/facebook_legacy_cookies.txt    (Netscape, cho yt-dlp)
Dùng cho DAG Facebook nhóm (cũ) legacy_fb_groups_crawler và bộ phân loại Qwen. DAG
social_crawler_fb dùng cookie riêng: scripts/import_browser_cookies.py facebook

Yêu cầu: chạy trên máy CÓ màn hình (không phải server headless).
"""

import os
import json
import time

from playwright.sync_api import sync_playwright

BASE_DIR     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_DIR       = os.path.join(BASE_DIR, 'cookies')
COOKIES_JSON = os.path.join(DB_DIR, 'facebook_legacy_cookies.json')
COOKIES_TXT  = os.path.join(DB_DIR, 'facebook_legacy_cookies.txt')

UA = ('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')


def _save_netscape(cookies, path):
    lines = ['# Netscape HTTP Cookie File']
    for c in cookies:
        domain      = c.get('domain', '')
        include_sub = 'TRUE' if domain.startswith('.') else 'FALSE'
        cpath       = c.get('path', '/')
        secure      = 'TRUE' if c.get('secure', False) else 'FALSE'
        expiry      = max(0, int(c.get('expires', 0)))
        name, value = c.get('name', ''), c.get('value', '')
        lines.append(f"{domain}\t{include_sub}\t{cpath}\t{secure}\t{expiry}\t{name}\t{value}")
    with open(path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))


def main():
    os.makedirs(DB_DIR, exist_ok=True)
    print("Mở trình duyệt — hãy ĐĂNG NHẬP Facebook thủ công...")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=False,
            args=['--no-sandbox', '--disable-blink-features=AutomationControlled'],
        )
        ctx = browser.new_context(user_agent=UA,
                                  viewport={'width': 1280, 'height': 800},
                                  locale='en-US')
        page = ctx.new_page()
        page.goto('https://www.facebook.com/', wait_until='domcontentloaded')

        # Chờ tối đa 5 phút cho tới khi đăng nhập xong (xuất hiện c_user)
        print("Đang chờ đăng nhập (tối đa 5 phút)...")
        deadline = time.time() + 300
        logged_in = False
        while time.time() < deadline:
            if any(c['name'] == 'c_user' for c in ctx.cookies()):
                logged_in = True
                break
            time.sleep(2)

        if not logged_in:
            print("✗ Hết thời gian — chưa phát hiện đăng nhập. Thử lại.")
            browser.close()
            return

        # Đợi thêm chút để FB set đầy đủ cookie phiên
        time.sleep(3)
        cookies = ctx.cookies()
        browser.close()

    with open(COOKIES_JSON, 'w') as f:
        json.dump(cookies, f)
    _save_netscape(cookies, COOKIES_TXT)

    names = {c['name'] for c in cookies}
    print(f"✓ Đã lưu {len(cookies)} cookie → {COOKIES_JSON}")
    print(f"  c_user: {'có' if 'c_user' in names else 'THIẾU'}, "
          f"xs: {'có' if 'xs' in names else 'THIẾU'}")
    print("Xong. Giờ có thể chạy lại DAG.")


if __name__ == '__main__':
    main()
