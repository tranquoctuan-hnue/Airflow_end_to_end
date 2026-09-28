from datetime import datetime

try:
    from zoneinfo import ZoneInfo
    TZ = ZoneInfo("Asia/Ho_Chi_Minh")
except Exception:
    try:
        import pytz
        TZ = pytz.timezone("Asia/Ho_Chi_Minh")
    except Exception:
        TZ = None
        print("Warning: zoneinfo and pytz not available — falling back to system local time.")

def _now():
    """Return current datetime localized to TZ if available, else system local now."""
    if TZ is None:
        return datetime.now()
    return datetime.now(TZ)