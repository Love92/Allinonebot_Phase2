# core/tide_gate.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Dict, Any
from datetime import datetime, timedelta, timezone

@dataclass
class TideGateConfig:
    tide_window_hours: float = 2.5
    entry_late_only: bool = False
    entry_late_from: float = 1.0
    entry_late_to: float = 2.5
    max_orders_per_day: int = 8
    max_orders_per_tide_window: int = 2
    counter_scope: str = "per_user"  # per_user | global
    lat: float = 32.7503
    lon: float = 129.8777

@dataclass
class TideInfo:
    type: Optional[str]
    center_ts: Optional[datetime]
    tau_hr: Optional[float]
    in_window: bool

@dataclass
class TideGateResult:
    ok: bool
    reason: Optional[str]
    tide_window_id: Optional[str]
    counters: Dict[str, Any]
    tide_info: TideInfo

def _local_today_key(now: datetime) -> str:
    return now.astimezone(timezone(timedelta(hours=7))).strftime("%Y-%m-%d")

async def _get_nearest_tide_event(storage, now: datetime, lat: float, lon: float):
    """
    TODO: Gắn vào data.moon_tide của anh.
    Nên trả: {"ts": datetime_utc, "type": "HIGH"|"LOW"} hoặc None
    """
    try:
        # ví dụ anh có sẵn: data.moon_tide.get_nearest_tide_event
        from data.moon_tide import get_nearest_tide_event  # type: ignore
        return await get_nearest_tide_event(now, lat=lat, lon=lon)  # nếu là async
    except Exception:
        return None

async def tide_gate_check(now: datetime, storage, cfg: TideGateConfig, scope_uid: Optional[int]=None) -> TideGateResult:
    tide = await _get_nearest_tide_event(storage, now, cfg.lat, cfg.lon)
    if not tide:
        return TideGateResult(False, "NO_TIDE_DATA", None, {}, TideInfo(None, None, None, False))

    center_ts = tide["ts"]
    tide_type = tide["type"]
    tau = abs((now - center_ts).total_seconds()) / 3600.0
    in_window = (tau <= float(cfg.tide_window_hours))
    tide_window_id = f'{center_ts.astimezone(timezone(timedelta(hours=7))).strftime("%Y%m%dT%H%M")}-{tide_type}'
    tide_info = TideInfo(tide_type, center_ts, round(tau,3), in_window)

    if not in_window:
        return TideGateResult(False, f"OUT_OF_TIDE_WINDOW_{tide_type}", tide_window_id, {}, tide_info)

    if cfg.entry_late_only:
        lf = float(cfg.entry_late_from); lt = float(cfg.entry_late_to)
        if not (lf <= tau <= lt):
            return TideGateResult(False, "OUT_OF_LATE_BAND", tide_window_id, {}, tide_info)

    scope = str(scope_uid) if (cfg.counter_scope=="per_user" and scope_uid is not None) else "GLOBAL"
    day_key = _local_today_key(now)

    used_day = await storage.get_counter(f"DAY:{scope}:{day_key}")
    used_tw  = await storage.get_counter(f"TW:{scope}:{tide_window_id}")

    if used_day >= int(cfg.max_orders_per_day):
        return TideGateResult(False, "MAX_ORDERS_PER_DAY_REACHED", tide_window_id,
                              {"day_used": used_day, "day_max": cfg.max_orders_per_day}, tide_info)

    if used_tw >= int(cfg.max_orders_per_tide_window):
        return TideGateResult(False, "MAX_ORDERS_PER_TW_REACHED", tide_window_id,
                              {"tw_used": used_tw, "tw_max": cfg.max_orders_per_tide_window}, tide_info)

    return TideGateResult(True, None, tide_window_id,
                          {"day_used": used_day, "day_max": cfg.max_orders_per_day,
                           "tw_used": used_tw, "tw_max": cfg.max_orders_per_tide_window}, tide_info)

async def bump_counters_after_execute(storage, tgr: TideGateResult, scope_uid: Optional[int]):
    scope = str(scope_uid) if scope_uid is not None else "GLOBAL"
    day_key = _local_today_key(datetime.now(timezone.utc))
    if tgr.tide_window_id:
        await storage.incr_counter(f"TW:{scope}:{tgr.tide_window_id}", 1)
    await storage.incr_counter(f"DAY:{scope}:{day_key}", 1)
