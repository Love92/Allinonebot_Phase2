# core/tide_gate.py
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional, Dict, Any, Tuple
from datetime import datetime, timedelta

from strategy.signal_generator import tide_window_now
from utils.time_utils import VN_TZ, now_vn


# =========================
# Config & Result dataclass
# =========================

@dataclass
class TideGateConfig:
    tide_window_hours: float = 2.5
    # Late-band: chỉ cho phép vào lệnh khi ở nửa sau khung (ví dụ từ 0.5h → 2.5h)
    entry_late_only: bool = False
    entry_late_from: float = 1.0
    entry_late_to: float = 2.5

    # Quota
    max_per_day: int = 8
    max_per_tide_window: int = 2

    # Đếm theo user hay toàn cục
    # per_user | global
    counter_scope: str = "per_user"

@dataclass
class TGateResult:
    ok: bool
    reason: str
    counters: Dict[str, Any]


# =========================
# Storage helpers (compat)
# =========================

async def _get_counter(storage, key: str) -> int:
    """
    Lấy bộ đếm từ storage. Hỗ trợ cả async/sync, fallback 0 nếu chưa có.
    """
    try:
        # async-style
        if hasattr(storage, "get_counter") and callable(storage.get_counter):
            val = storage.get_counter(key)
            if hasattr(val, "__await__"):
                val = await val  # type: ignore
            return int(val or 0)
    except Exception:
        pass

    try:
        # sync-style
        if hasattr(storage, "get_counter") and callable(storage.get_counter):
            return int(storage.get_counter(key) or 0)  # type: ignore
    except Exception:
        pass

    # very fallback
    try:
        d = getattr(storage, "data", {})
        return int(d.get("_counters", {}).get(key, 0))
    except Exception:
        return 0


async def _incr_counter(storage, key: str, delta: int = 1) -> None:
    """
    Tăng bộ đếm trong storage. Hỗ trợ async/sync.
    """
    try:
        if hasattr(storage, "incr_counter") and callable(storage.incr_counter):
            rv = storage.incr_counter(key, delta)
            if hasattr(rv, "__await__"):
                await rv  # type: ignore
            return
    except Exception:
        pass

    # very fallback: tự ghi vào storage.data
    try:
        if not hasattr(storage, "data"):
            return
        d = storage.data.setdefault("_counters", {})
        d[key] = int(d.get(key, 0)) + int(delta)
        if hasattr(storage, "persist") and callable(storage.persist):
            storage.persist()
    except Exception:
        pass


# =========================
# Core: TideGate check
# =========================

def _to_vn(dt: datetime) -> datetime:
    """Chuyển mọi thời điểm về VN_TZ (aware)."""
    if getattr(dt, "tzinfo", None) is None:
        return VN_TZ.localize(dt)
    return dt.astimezone(VN_TZ)


def _center_from_window(twin: Tuple[datetime, datetime]) -> datetime:
    start, end = twin
    # bảo đảm đều là VN_TZ
    s = _to_vn(start)
    e = _to_vn(end)
    return s + (e - s) / 2


async def tide_gate_check(*, now: datetime, storage, cfg: TideGateConfig, scope_uid: Optional[int] = None) -> TGateResult:
    """
    Kiểm tra quota 2/khung & 8/ngày và các ràng buộc khác (late-band).
    - Chuẩn hoá 'now' về VN_TZ trước khi tính khung.
    - Nếu thiếu dữ liệu tide và ALLOW_NO_TIDE_DATA=true: fallback center=now (VN_TZ),
      khung ±tide_window_hours (vẫn đảm bảo quota).
    """
    # 1) Chuẩn hoá thời điểm hiện tại (VN)
    now_local = _to_vn(now)

    # 2) Lấy khung thủy triều theo dữ liệu chuẩn
    twin = None
    try:
        twin = tide_window_now(now_local, hours=float(cfg.tide_window_hours))
    except Exception:
        twin = None

    reason_tag = "OK"
    if twin is None:
        # 2b) Fallback nếu được phép
        allow_fb = str(os.getenv("ALLOW_NO_TIDE_DATA", "false")).strip().lower() in ("1", "true", "yes", "on", "y")
        if not allow_fb:
            return TGateResult(ok=False, reason="NO_TIDE_DATA", counters={})
        # Dựng khung tạm ±hours
        h = float(cfg.tide_window_hours)
        start = now_local - timedelta(hours=h)
        end = now_local + timedelta(hours=h)
        twin = (start, end)
        center = now_local
        reason_tag = "FALLBACK_NO_TIDE_DATA"
    else:
        # 2a) Tính center =
        center = _center_from_window(twin)

    start, end = twin
    # 3) Xác định trong/ngoài khung + tau (khoảng cách giờ tới center)
    in_window = start <= now_local <= end
    tau_hr = abs((now_local - center).total_seconds()) / 3600.0

    if not in_window:
        return TGateResult(
            ok=False,
            reason="OUT_OF_TIDE_WINDOW",
            counters={"window": f"{start.strftime('%H:%M')}–{end.strftime('%H:%M')}", "center": center.strftime("%H:%M")},
        )

    # 4) Late-band
    if cfg.entry_late_only:
        lf, lt = float(cfg.entry_late_from), float(cfg.entry_late_to)
        if not (lf <= tau_hr <= lt):
            return TGateResult(
                ok=False,
                reason="OUT_OF_LATE_BAND",
                counters={
                    "tau_hr": round(tau_hr, 3),
                    "late_from": lf,
                    "late_to": lt,
                    "center": center.strftime("%H:%M"),
                    "window": f"{start.strftime('%H:%M')}–{end.strftime('%H:%M')}",
                },
            )

    # 5) Build scope & keys
    scope = str(scope_uid) if (cfg.counter_scope == "per_user" and scope_uid is not None) else "GLOBAL"
    day_key = center.strftime("%Y-%m-%d")               # theo ngày (VN)
    win_key = center.strftime("%Y-%m-%d %H:%M")         # khoá theo tâm khung (VN)

    # 6) Lấy counters hiện tại
    used_day = await _get_counter(storage, f"DAY:{scope}:{day_key}")
    used_win = await _get_counter(storage, f"TW:{scope}:{win_key}")

    # 7) So sánh quota
    max_day = int(cfg.max_per_day)
    max_win = int(cfg.max_per_tide_window)

    if used_day >= max_day:
        return TGateResult(
            ok=False,
            reason="DAY_LIMIT",
            counters={
                "used_day": used_day,
                "max_day": max_day,
                "window": f"{start.strftime('%H:%M')}–{end.strftime('%H:%M')}",
                "center": center.strftime("%H:%M"),
                "day_key": day_key,
                "win_key": win_key,
            },
        )

    if used_win >= max_win:
        return TGateResult(
            ok=False,
            reason="WINDOW_LIMIT",
            counters={
                "used_win": used_win,
                "max_win": max_win,
                "window": f"{start.strftime('%H:%M')}–{end.strftime('%H:%M')}",
                "center": center.strftime("%H:%M"),
                "day_key": day_key,
                "win_key": win_key,
            },
        )

    # 8) OK
    return TGateResult(
        ok=True,
        reason=reason_tag,
        counters={
            "used_day": used_day, "max_day": max_day,
            "used_win": used_win, "max_win": max_win,
            "window": f"{start.strftime('%H:%M')}–{end.strftime('%H:%M')}",
            "center": center.strftime("%H:%M"),
            "day_key": day_key,
            "win_key": win_key,
            "tau_hr": round(tau_hr, 3),
        },
    )


# =========================
# Bump counters sau khi khớp
# =========================

async def bump_counters_after_execute(storage, tgr: TGateResult, scope_uid: Optional[int] = None) -> None:
    """
    Gọi sau khi lệnh THỰC SỰ khớp (opened_real=True).
    Dùng day_key/win_key đã trả về từ tide_gate_check; nếu không có thì build lại từ now_vn().
    """
    try:
        scope = "GLOBAL" if scope_uid is None else str(scope_uid)
        day_key = (tgr.counters or {}).get("day_key")
        win_key = (tgr.counters or {}).get("win_key")

        if not (day_key and win_key):
            center = now_vn()
            day_key = center.strftime("%Y-%m-%d")
            win_key = center.strftime("%Y-%m-%d %H:%M")

        await _incr_counter(storage, f"DAY:{scope}:{day_key}", 1)
        await _incr_counter(storage, f"TW:{scope}:{win_key}", 1)
    except Exception:
        # tránh làm vỡ flow nếu counter lỗi
        pass
