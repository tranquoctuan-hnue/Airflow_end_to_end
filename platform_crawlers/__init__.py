"""
platform_crawlers — discovery URL video cho từng mạng xã hội.

Scraper chỉ TÌM URL. Việc dedup 4 tầng + filter CCTV + download đều do
crawler_core.pipeline.VideoPipeline xử lý, nên mọi platform bắt buộc đi qua
cùng một cổng.

    from platform_crawlers import get_scraper
    scraper = get_scraper('youtube')
    for urls, label in scraper.discover():
        ...
"""

from platform_crawlers.scrapers import (
    BaseScraper,
    get_scraper,
    ALL_PLATFORMS,
)

__all__ = ['BaseScraper', 'get_scraper', 'ALL_PLATFORMS']
