"""Discover currently-live CCTV-style video streams on YouTube via yt-dlp search."""

from typing import Dict, List, Optional

from yt_dlp import YoutubeDL

from src.modules.logger import default_logger as logger
from .config import RESULTS_PER_KEYWORD
from .keyword_generator import generate_search_keywords
from .title_filter import is_likely_cctv_title


def find_live_streams(
    keywords: Optional[List[str]] = None,
    results_per_keyword: Optional[int] = None,
) -> List[Dict[str, str]]:
    """
    Search YouTube (ytsearchN:<keyword>) for videos currently live, using
    extract_flat search so no per-video network call is needed to know
    live status.

    Returns a list of {"video_id", "url", "title", "channel_id"}, deduplicated by
    video_id (a video can match more than one keyword).

    Nếu không truyền `keywords`, sinh từ khóa bằng Qwen2.5-VL (ngôn ngữ ngẫu nhiên
    mỗi lần gọi) qua generate_search_keywords() — xem youtube_live/config.py
    (USE_LLM_KEYWORDS, KEYWORD_LANGUAGES).
    """
    keywords = keywords or generate_search_keywords()
    results_per_keyword = results_per_keyword or RESULTS_PER_KEYWORD

    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "extract_flat": "in_playlist",
    }

    seen_ids = set()
    live_streams = []
    title_rejected = 0

    with YoutubeDL(ydl_opts) as ydl:
        for keyword in keywords:
            query = f"ytsearch{results_per_keyword}:{keyword}"
            try:
                info = ydl.extract_info(query, download=False)
            except Exception as e:
                logger.error(f"YouTube search failed for '{keyword}': {e}")
                continue

            for entry in (info or {}).get("entries", []) or []:
                if not entry:
                    continue
                video_id = entry.get("id")
                if not video_id or video_id in seen_ids:
                    continue
                if entry.get("live_status") != "is_live":
                    continue

                title = entry.get("title") or ""
                if not is_likely_cctv_title(title):
                    seen_ids.add(video_id)
                    title_rejected += 1
                    logger.debug(f"[SKIP - title filter] {title}")
                    continue

                seen_ids.add(video_id)
                live_streams.append({
                    "video_id": video_id,
                    "url": entry.get("url") or f"https://www.youtube.com/watch?v={video_id}",
                    "title": title,
                    "channel_id": entry.get("channel_id") or "",
                })

    logger.info(
        f"Found {len(live_streams)} live streams across {len(keywords)} keywords "
        f"(rejected by title filter: {title_rejected})"
    )
    return live_streams
