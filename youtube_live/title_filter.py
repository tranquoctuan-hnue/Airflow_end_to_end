"""Cheap, no-network heuristic to pre-filter obviously non-CCTV search results by title."""

# Các từ khoá thường gặp ở video giải trí/tổng hợp — không phải footage CCTV thật,
# dù title có match keyword search (vd: "Live Cameras Around the World - Music, Timelapse, Travel").
BLOCKLIST_KEYWORDS = [
    "music", "nhạc", "documentary", "compilation", "relaxing", "relax",
    "asmr", "sleep", "meditation", "lo-fi", "lofi", "top 10", "best of",
    "movie", "trailer", "gameplay", "vlog", "review", "reaction",
    "podcast", "tutorial", "highlights", "concert", "dj set",
]


def is_likely_cctv_title(title: str) -> bool:
    """
    Trả về False nếu title chứa từ khoá cho thấy đây là video giải trí/tổng hợp
    thay vì footage camera trực tiếp thật. Đây chỉ là bộ lọc rẻ, chạy trước khi
    tốn chi phí resolve stream + chụp frame + chạy model phân loại ảnh.
    """
    lowered = title.lower()
    return not any(term in lowered for term in BLOCKLIST_KEYWORDS)
