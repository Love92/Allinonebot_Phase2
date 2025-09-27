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
    # Late-band: chỉ cho phép vào lệnh khi ở nửa sau khung
    entry_late_only: bool = False
    entry_late_from: float = 1.0
    entry_late_to: float = 2.5

    # Quota
    max_per_day: int = 8
    max_per_tide_window: int = 2

    # Đếm theo user hay toàn cục: "per_user" | "global"
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
    """Lấy bộ đếm từ storage (hỗ trợ async/sync)."""
    try:
        if hasattr(storage, "get_counter") and callable(storage.get_counter):
            val = storage.get_counter(key)
            if hasattr(val, "__await__"):
                val = await val  # type: ignore
            return int(val or 0)
    except Exception:
        pass

    try:
        if hasattr(storage, "get_counter") and callable(storage.get_counter):
            return int(storage.get_counter(key) or 0)  # type: ignore
    except Exception:
        pass

    try:
        d = getattr(storage, "data", {})
        return int(d.get("_counters", {}).get(key, 0))
    except Exception:
        return 0


async def _incr_counter(storage, key: str, delta: int = 1) -> None:
    """Tăng bộ đếm trong storage (hỗ trợ async/sync)."""
    try:
        if hasattr(storage, "incr_counter") and callable(storage.incr_counter):
            rv = storage.incr_counter(key, delta)
            if hasattr(rv, "__await__"):
                await rv  # type: ignore
            return
    except Exception:
        pass

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
# Internal helpers
# =========================

def _to_vn(dt: datetime) -> datetime:
    """Chuyển mọi thời điểm về VN_TZ (aware)."""
    if getattr(dt, "tzinfo", None) is None:
        return VN_TZ.localize(dt)
    return dt.astimezone(VN_TZ)


def _center_from_window(twin: Tuple[datetime, datetime]) -> datetime:
    start, end = twin
    s = _to_vn(start)
    e = _to_vn(end)
    return s + (e - s) / 2


def _fmt_hhmm(dt: datetime) -> str:
    return _to_vn(dt).strftime("%H:%M")


def _fmt_day(dt: datetime) -> str:
    return _to_vn(dt).strftime("%Y-%m-%d")


# =========================
# Core: TideGate check
# =========================

async def tide_gate_check(*, now: datetime, storage, cfg: TideGateConfig, scope_uid: Optional[int] = None) -> TGateResult:
    """
    Kiểm tra quota 2/khung & 8/ngày + late-band, dùng CHÍNH XÁC cách xác định khung như /report:
      - Chuẩn hoá 'now' → VN_TZ
      - Gọi đúng 1 lần: tide_window_now(now_local, hours=cfg.tide_window_hours)
      - Nếu None: trả NO_TIDE_DATA (trừ khi ALLOW_NO_TIDE_DATA=true → fallback ±hours quanh 'now_local')
    """
    # 1) Chuẩn hoá thời điểm hiện tại (VN)
    now_local = _to_vn(now)

    # 2) Lấy khung thuỷ triều đúng như /report
    try:
        twin = tide_window_now(now_local, hours=float(cfg.tide_window_hours))
    except Exception:
        twin = None

    reason_tag = "OK"
    if twin is None:
        # tôn trọng dữ liệu: không “đoán” khung, chỉ fallback khi được bật cờ
        allow_fb = str(os.getenv("ALLOW_NO_TIDE_DATA", "false")).strip().lower() in ("1", "true", "yes", "on", "y")
        if not allow_fb:
            return TGateResult(ok=False, reason="NO_TIDE_DATA", counters={})
        # Fallback: ±hours quanh now_local
        h = float(cfg.tide_window_hours)
        start = now_local - timedelta(hours=h)
        end = now_local + timedelta(hours=h)
        twin = (start, end)
        center = now_local
        reason_tag = "FALLBACK_NO_TIDE_DATA"
    else:
        center = _center_from_window(twin)

    start, end = twin

    # 3) In-window & late-band
    if not (start <= now_local <= end):
        return TGateResult(
            ok=False,
            reason="OUT_OF_TIDE_WINDOW",
            counters={"window": f"{_fmt_hhmm(start)}–{_fmt_hhmm(end)}", "center": _fmt_hhmm(center)},
        )

    tau_hr = abs((now_local - center).total_seconds()) / 3600.0
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
                    "center": _fmt_hhmm(center),
                    "window": f"{_fmt_hhmm(start)}–{_fmt_hhmm(end)}",
                },
            )

    # 4) Keys & counters
    scope = str(scope_uid) if (cfg.counter_scope == "per_user" and scope_uid is not None) else "GLOBAL"
    day_key = _fmt_day(center)                              # theo ngày (VN)
    win_key = f"{_fmt_day(center)} {_fmt_hhmm(center)}"     # khoá theo tâm khung (VN)

    used_day = await _get_counter(storage, f"DAY:{scope}:{day_key}")
    used_win = await _get_counter(storage, f"TW:{scope}:{win_key}")

    max_day = int(cfg.max_per_day)
    max_win = int(cfg.max_per_tide_window)

    if used_day >= max_day:
        return TGateResult(
            ok=False,
            reason="DAY_LIMIT",
            counters={
                "used_day": used_day,
                "max_day": max_day,
                "window": f"{_fmt_hhmm(start)}–{_fmt_hhmm(end)}",
                "center": _fmt_hhmm(center),
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
                "window": f"{_fmt_hhmm(start)}–{_fmt_hhmm(end)}",
                "center": _fmt_hhmm(center),
                "day_key": day_key,
                "win_key": win_key,
            },
        )

    # 5) OK
    return TGateResult(
        ok=True,
        reason=reason_tag,
        counters={
            "used_day": used_day, "max_day": max_day,
            "used_win": used_win, "max_win": max_win,
            "window": f"{_fmt_hhmm(start)}–{_fmt_hhmm(end)}",
            "center": _fmt_hhmm(center),
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
            day_key = _fmt_day(center)
            win_key = f"{_fmt_day(center)} {_fmt_hhmm(center)}"

        await _incr_counter(storage, f"DAY:{scope}:{day_key}", 1)
        await _incr_counter(storage, f"TW:{scope}:{win_key}", 1)
    except Exception:
        # tránh làm vỡ flow nếu counter lỗi
        pass
