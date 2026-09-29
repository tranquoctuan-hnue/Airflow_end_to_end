#!/usr/bin/env python3
"""
Nhập cookie từ browser BẠN ĐANG DÙNG HẰNG NGÀY vào crawler.

VÌ SAO CẦN CÁCH NÀY
───────────────────
Đăng nhập bằng Playwright (đã bỏ) thất bại khi
platform dùng "Continue with Google": Google phát hiện browser bị điều khiển
tự động và chặn với thông báo
    "Couldn't sign you in — This browser or app may not be secure"
Không có cách vượt đáng tin cậy, và cũng không nên vượt.

Cách này không đăng nhập tự động gì cả — chỉ đọc cookie từ profile browser
thật, nơi bạn đã đăng nhập như người dùng bình thường. Google/TikTok/X không
thấy có gì bất thường vì phiên đăng nhập đó do chính bạn tạo bằng tay.

CÁCH DÙNG
─────────
    # Xem browser nào đang có cookie đăng nhập platform nào
    python scripts/import_browser_cookies.py --list

    # Nhập tất cả platform từ Chrome profile Default
    python scripts/import_browser_cookies.py

    # Chỉ định profile / browser khác
    python scripts/import_browser_cookies.py --browser chrome --profile "Profile 1"
    python scripts/import_browser_cookies.py --browser firefox

    # Chỉ một vài platform
    python scripts/import_browser_cookies.py tiktok x

Ghi ra 2 định dạng vì hai công cụ đọc khác nhau:
    cookies/<platform>_cookies.txt   → yt-dlp (download)
    cookies/<platform>_cookies.json  → Playwright (discovery)

LƯU Ý: cookie là thông tin đăng nhập. Script không in giá trị cookie ra
terminal, chỉ in tên. File sinh ra chứa secret thật — đừng commit vào git.
"""

import argparse
import os
import sys
import time

REPO_ROOT   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

from platform_crawlers.browser_cookies import (  # noqa: E402
    COOKIES_DIR, PLATFORMS, _chromium_profiles, cookies_for, load_jar,
    normalize_expires, to_netscape, to_playwright, write_cookie_files,
)

def do_list():
    candidates: list[tuple[str, str | None]] = []
    for browser in ('chrome', 'chromium', 'brave', 'edge'):
        candidates += [(browser, p) for p in _chromium_profiles(browser)]
    candidates.append(('firefox', None))

    print('Quét cookie đăng nhập trong các browser/profile...\n')
    found_any = False

    for browser, profile in candidates:
        tag = f"{browser}" + (f" [{profile}]" if profile else " [Default]")
        try:
            jar = load_jar(browser, profile)
        except Exception:
            continue        # browser/profile không tồn tại → bỏ qua im lặng

        rows = []
        for plat, (domains, keys) in PLATFORMS.items():
            cks   = cookies_for(jar, domains)
            names = {c.name for c in cks}
            hits  = [k for k in keys if k in names]
            if hits:
                rows.append((plat, len(cks), hits))

        if rows:
            found_any = True
            print(f"  {tag}  ({len(jar)} cookie tổng)")
            for plat, n, hits in sorted(rows):
                print(f"      ✓ {plat:12} {n:3} cookie   đã login ({', '.join(hits)})")
            print()

    if not found_any:
        print('  Không tìm thấy profile nào có cookie đăng nhập.')
        print('  Hãy mở browser thường của bạn, đăng nhập các platform, rồi chạy lại.')
    return 0


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('platforms', nargs='*',
                    help=f"platform cần nhập (mặc định tất cả): {', '.join(PLATFORMS)}")
    ap.add_argument('--browser', default='chrome',
                    help='chrome | firefox | brave | edge | chromium | opera | vivaldi')
    ap.add_argument('--profile', default=None,
                    help='tên profile, ví dụ "Profile 1" (mặc định Default)')
    ap.add_argument('--list', action='store_true',
                    help='chỉ liệt kê browser nào có cookie đăng nhập gì')
    args = ap.parse_args()

    if args.list:
        return do_list()

    wanted = args.platforms or list(PLATFORMS)
    unknown = [p for p in wanted if p not in PLATFORMS]
    if unknown:
        print(f"Platform không hỗ trợ: {unknown}. Có: {', '.join(PLATFORMS)}")
        return 1

    tag = f"{args.browser}" + (f" [{args.profile}]" if args.profile else " [Default]")
    try:
        jar = load_jar(args.browser, args.profile)
    except Exception as e:
        print(f"Không đọc được cookie từ {tag}: {type(e).__name__}: {e}")
        print("Thử: python scripts/import_browser_cookies.py --list")
        return 1

    os.makedirs(COOKIES_DIR, exist_ok=True)
    print(f"Nguồn: {tag} — {len(jar)} cookie tổng")
    print('-' * 68)

    saved = skipped = 0
    for plat in wanted:
        domains, keys = PLATFORMS[plat]
        cks   = cookies_for(jar, domains)
        names = {c.name for c in cks}
        hits  = [k for k in keys if k in names]

        if not hits:
            reason = 'chưa đăng nhập' if cks else 'không có cookie'
            print(f"  {plat:12} — bỏ qua ({reason}, cần: {'/'.join(keys)})")
            skipped += 1
            continue

        try:
            write_cookie_files(plat, cks)
            if plat == 'facebook':
                # DAG cũ legacy_fb_groups_crawler + bộ lọc Qwen đọc bản riêng (yt-dlp ghi
                # ngược vào file đó, nên KHÔNG dùng chung với facebook_cookies.*)
                write_cookie_files(plat, cks, name='facebook_legacy')
        except Exception as e:
            print(f"  {plat:12} ✗ ghi file thất bại: {e}")
            continue

        # Cookie hết hạn sớm nhất → biết khi nào cần nhập lại
        now    = time.time()
        future = [e for e in (normalize_expires(c.expires) for c in cks)
                  if e > now]
        soonest = min(future) if future else None
        when = (
            f", hạn sớm nhất {time.strftime('%Y-%m-%d', time.localtime(soonest))}"
            if soonest else ''
        )
        print(f"  {plat:12} ✓ {len(cks):3} cookie  ({', '.join(hits)}){when}")
        saved += 1

    print('-' * 68)
    print(f"Đã ghi {saved} platform, bỏ qua {skipped}.")
    print(f"Thư mục: {COOKIES_DIR}  (chmod 600 — chứa thông tin đăng nhập)")
    if skipped:
        print()
        print("Platform bị bỏ qua: mở browser THƯỜNG của bạn, đăng nhập platform đó")
        print("bằng tay (không qua Playwright), rồi chạy lại script này.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
