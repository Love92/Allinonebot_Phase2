# ----------------------- utils/time_utils.py -----------------------
from __future__ import annotations
from datetime import datetime, timedelta, timezone
import pytz

TOKYO_TZ = pytz.timezone('Asia/Tokyo')
VN_TZ = pytz.timezone('Asia/Ho_Chi_Minh')

def now_vn() -> datetime:
    return datetime.now(VN_TZ)

def to_vn(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(VN_TZ)

def to_tokyo(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(TOKYO_TZ)

def within_window(now_dt: datetime, center_dt: datetime, half_hours: float) -> bool:
    delta = timedelta(hours=half_hours)
    return (center_dt - delta) <= now_dt <= (center_dt + delta)
