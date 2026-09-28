---
name: add-platform
description: Thêm một nguồn video mới (TikTok, Bilibili, Vimeo, Instagram, trang tin…) vào crawler — scraper, luật canonical ID cho dedup L1, cookie, DAG riêng, đưa vào vòng xoay. Dùng khi người dùng muốn crawl thêm platform/website, hoặc sửa cách một platform tìm URL.
---

# Thêm platform mới

Nguyên tắc: **scraper chỉ tìm URL**. Tải, lọc, dedup và ghi DB đều do
`crawler_core.pipeline.VideoPipeline` lo. Làm đúng các bước dưới đây thì platform mới tự
có đủ 4 tầng dedup, mọi bộ lọc, Chế độ test, form Trigger và vòng xoay.

## Các bước

1. **Luật canonical ID (dedup L1):** `crawler_core/dedup.py` → `_PLATFORM_RULES`.
   ```python
   'bilibili': (
       {'bilibili.com', 'b23.tv'},                    # host nhận diện platform
       [(r'/video/(BV[0-9A-Za-z]{10})', 'video', False),
        (r'b23\.tv/([0-9A-Za-z]+)',     'video', True)],   # True = short link, resolve trước L1
   ),
   ```
   Kiểm tra: `extract_canonical_id('<url mẫu>')` phải trả `('bilibili', 'video', 'BV…')`.
   Thiếu luật này thì L1 không bắt được trùng, dù L2–L4 vẫn bắt được sau khi đã tải.

2. **Hàm tìm URL:** `platform_crawlers/discovery.py`. Có sẵn ba loại dùng lại được:
   - `ytdlp_discover(target, limit, platform, cookies_file)`: nếu yt-dlp có extractor
     tìm kiếm/playlist cho platform đó. Đây là cách rẻ nhất; kiểm tra trước bằng
     `yt-dlp --flat-playlist "<url trang tìm kiếm>"`.
   - `playwright_discover(page_url, link_pattern, limit, cookies_json, max_scrolls, timeout_s)`:
     cuộn trang bằng trình duyệt và bắt href theo regex.
   - `discover_with_fallback(...)`: thử yt-dlp trước, Playwright sau.

   **URL trả về phải tuyệt đối** (`https://…`). Pipeline chặn và báo lỗi với URL tương
   đối.

3. **Scraper:** `platform_crawlers/scrapers.py`. Thêm một class rồi đăng ký vào
   `_REGISTRY`:
   ```python
   class BilibiliScraper(BaseScraper):
       platform = 'bilibili'          # phải khớp key trong _PLATFORM_RULES
       needs_cookies = False

       def discover(self):
           by_label = keywords.phrases_by_label(None)      # {nhãn: [từ khóa]} (Gemini)
           for label, kw in self.iter_label_keywords(by_label):   # tự bỏ nhãn đủ quota / hết giờ
               urls = playwright_discover(f'https://search.bilibili.com/all?keyword={quote(kw)}',
                                          link_pattern=r'/video/BV[0-9A-Za-z]{10}',
                                          limit=self.max_per_target, cookies_json=None,
                                          max_scrolls=cfg.MAX_SCROLLS, timeout_s=cfg.DISCOVERY_TIMEOUT)
               yield urls, f'bilibili:{label}:{kw}', label   # (urls, nguồn, nhãn)
   ```
   **Luôn lặp qua `iter_label_keywords()`**, không tự lặp `by_label.items()`. Hàm này sắp
   nhãn thiếu video lên trước, bỏ nhãn đã đủ quota và dừng khi hết time budget. Platform
   tìm theo hashtag (TikTok, Instagram) thì dùng `keywords.hashtags_by_label()`.

4. **Cấu hình:** `platform_crawlers/config.py`. Thêm tên platform vào
   `ENABLED_PLATFORMS`. Tham số riêng thì dùng `_env_int`/`_env_list` để ghi đè được
   bằng biến môi trường. Nếu cần giới hạn thời lượng riêng, thêm vào
   `PLATFORM_MAX_DURATION_S`.

5. **Cookie** (nếu cần đăng nhập): thêm platform vào `_COOKIE_PATHS` trong
   `crawler_core/downloader.py`, và thêm vào `PLATFORMS` trong
   `scripts/import_browser_cookies.py` hai thứ: các domain cần lấy, và tên cookie chứng
   tỏ đã đăng nhập (ví dụ `'x': (['.x.com', '.twitter.com'], ['auth_token', 'ct0'])`). Có cookie thì đặt `needs_cookies = True`, để task tự skip kèm hướng dẫn khi
   thiếu cookie.

6. **Tải video:** yt-dlp đủ cho phần lớn platform, qua `GenericDownloader`. Nếu phải tải
   kiểu riêng như Facebook DASH, viết class có `fetch(url) -> đường dẫn trong staging`
   (mẫu: `crawler_core/facebook_downloader.py`), rồi rẽ nhánh trong
   `task_crawl_platform()` ở `dags/social_crawler_common.py`.

7. **DAG:** tạo `dags/social_crawler_bilibili_dag.py`, chép mẫu từ
   `social_crawler_youtube_dag.py`. File **phải chứa chữ `airflow`** (`import airflow`),
   nếu không Airflow sẽ âm thầm bỏ qua file. Nếu `dag_id` khác `social_crawler_<platform>`,
   thêm vào `_DAG_ID_OVERRIDES`.

8. **Vòng xoay:** thêm platform vào `order` của Variable `crawler_rotation`, và sửa giá
   trị mặc định trong `start_airflow.sh` nếu muốn.

## Kiểm tra

1. `airflow_venv/bin/python scripts/doctor.py` phải báo DAG mới parse được.
2. Trigger DAG mới với **Chế độ test** và trần thời gian 2 phút, xem skill `test-pipeline`.
3. Đọc log lượt chạy:
   - dòng `PLATFORM:` và `XONG […]` với `seen`/`downloaded` khác 0;
   - các dòng `[LOẠI …]` có lý do hợp lý;
   - trong `data/.test_runs/<lượt>/` có video.
4. Viết thêm skill `<platform>-dag` (theo mẫu `youtube-dag`) ghi lại các bẫy của platform
   đó: cookie, rate limit, định dạng URL.
