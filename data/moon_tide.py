# ----------------------- data/moon_tide.py -----------------------
from __future__ import annotations
import json
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Optional, Dict, Any
import requests

from config.settings import WEATHERAPI_KEY, WORLDTIDES_KEY, LAT, LON
from utils.time_utils import TOKYO_TZ  # JST cho dữ liệu thủy triều Nagasaki

CACHE_FILE = "tide_moon_cache.json"

# ==== Moon cycle constants =======================================
T_LUNAR = 29.53  # days

# 8 anchor points (ngày tính từ New Moon)
ANCHORS = {
    "N":  0.00,                 # New Moon
    "WC": T_LUNAR / 8 * 1,      # Waxing Crescent
    "FQ": T_LUNAR / 8 * 2,      # First Quarter
    "WG": T_LUNAR / 8 * 3,      # Waxing Gibbous
    "F":  T_LUNAR / 8 * 4,      # Full Moon
    "Wg": T_LUNAR / 8 * 5,      # Waning Gibbous
    "LQ": T_LUNAR / 8 * 6,      # Last Quarter
    "Wc": T_LUNAR / 8 * 7,      # Waning Crescent
}
MAIN_ANCHORS = ["N", "FQ", "F", "LQ"]

ANCHOR_LABEL = {"N": "New Moon", "FQ": "First Quarter", "F": "Full Moon", "LQ": "Last Quarter"}
ANCHOR_EMOJI  = {"N": "🌑", "FQ": "🌓", "F": "🌕", "LQ": "🌗"}

# ==== Preset mapping theo % độ rọi (và hướng) ====================
# P2 dùng khi 25–75% & waxing; P4 dùng khi 25–75% & waning.
PRESETS: Dict[str, Dict[str, Any]] = {
    "P1": {
        "range": (0, 25),
        "label": "Waning Crescent - New Moon - Waxing Crescent",
        "suggestions": [
            "Short về New Moon (đáy), chuẩn bị đảo chiều",
            "Long hồi phục sau New Moon",
            "Nếu vol yếu → đứng ngoài, chờ xác nhận nến đảo",
        ],
    },
    "P2": {
        "range": (25, 75),
        "label": "Waxing Crescent - First Quarter - Waxing Gibbous",
        "suggestions": [
            "Long continuation theo xu hướng chính",
            "Giảm size quanh First Quarter (dễ sideway/điều chỉnh)",
        ],
    },
    "P3": {
        "range": (75, 100),
        "label": "Waxing Gibbous - Full Moon - Waning Gibbous",
        "suggestions": [
            "Long tiếp tới gần Full Moon (có sóng cuối)",
            "Full Moon là vùng đảo → giảm size hoặc chuyển Short",
            "Nếu trend H4/M30 vẫn mạnh → giữ Long nhưng đặt SL chặt",
        ],
    },
    "P4": {
        # Dải % trùng P2 nhưng áp dụng cho waning (giảm sáng)
        "range": (25, 75),
        "label": "Waning Gibbous - Last Quarter - Waning Crescent",
        "suggestions": [
            "Short continuation (giảm/sideway phân phối)",
            "Quanh Last Quarter → đứng ngoài hoặc Short nhỏ",
            "Giữ Short tới New Moon → reset chu kỳ",
        ],
    },
}

# ==== Cache helpers ==============================================
def _load_cache() -> dict:
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_cache(data: dict) -> None:
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

# ==== External APIs ==============================================
def get_moon_phase(date_str: str) -> Tuple[str, int]:
    """
    Input: date_str 'YYYY-MM-DD' (JST ngày đó)
    Returns: (phase_name, illumination_percent_int)
    """
    cache = _load_cache()
    if date_str in cache.get("moon_phase", {}):
        val = cache["moon_phase"][date_str]
        if isinstance(val, (list, tuple)) and len(val) >= 2:
            return str(val[0]), int(val[1])
        if isinstance(val, dict):
            return str(val.get("phase", "")), int(val.get("illum", 0))

    url = f"http://api.weatherapi.com/v1/astronomy.json?key={WEATHERAPI_KEY}&q=Nagasaki&dt={date_str}"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    data = r.json()
    astro = ((data or {}).get("astronomy", {}) or {}).get("astro", {}) or {}
    phase = str(astro.get("moon_phase", ""))
    illum = int(str(astro.get("moon_illumination", "0")) or 0)

    cache.setdefault("moon_phase", {})[date_str] = (phase, illum)
    _save_cache(cache)
    return phase, illum

def get_tide_events(date_str: str) -> List[str]:
    """
    Trả về list: ["Low HH:MM", "High HH:MM"] theo JST (Nagasaki).
    """
    cache = _load_cache()
    if date_str in cache.get("tide_data", {}):
        return list(cache["tide_data"][date_str])

    url = (
        "https://www.worldtides.info/api/v3"
        f"?extremes=true&lat={LAT}&lon={LON}&days=7&key={WORLDTIDES_KEY}"
    )
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    data = r.json()

    out: List[str] = []
    requested = datetime.strptime(date_str, "%Y-%m-%d").date()
    for t in (data or {}).get("extremes", []) or []:
        try:
            dt_local = datetime.fromtimestamp(int(t["dt"]), timezone.utc).astimezone(TOKYO_TZ)
            if dt_local.date() == requested:
                typ = str(t.get("type", "")).capitalize()
                out.append(f"{typ} {dt_local.strftime('%H:%M')}")
        except Exception:
            continue

    if not out:
        out = ["No tide data"]

    cache.setdefault("tide_data", {})[date_str] = out
    _save_cache(cache)
    return out

# ==== Core helpers =================================================
def _forward_delta_days(cur_age: float, anchor_age: float) -> float:
    """Số ngày từ cur_age tới anchor_age (đi tới phía trước theo chu kỳ)."""
    if anchor_age >= cur_age:
        return anchor_age - cur_age
    return T_LUNAR - (cur_age - anchor_age)

def _signed_circ_diff(age: float, A: float) -> float:
    """Sai khác có dấu trong [-T/2, +T/2]. <0: pre; >0: post."""
    d = age - A
    half = T_LUNAR / 2.0
    if d > half: d -= T_LUNAR
    elif d < -half: d += T_LUNAR
    return d

def _nearest_main_anchor(age: float) -> Tuple[str, float]:
    """Mốc gần nhất trong các mốc chính N/FQ/F/LQ."""
    best_key, best_dist = "N", 1e9
    for k in MAIN_ANCHORS:
        d = abs(_signed_circ_diff(age, ANCHORS[k]))
        if d < best_dist:
            best_key, best_dist = k, d
    return best_key, best_dist

# ==== Age estimation ==============================================
def estimate_age(illum: int, date_iso: str) -> float:
    """
    Ước lượng tuổi trăng theo độ rọi + hướng tăng/giảm so với hôm qua.
    Waxing: 0..100% → 0..Full; Waning: 100..0% → Full..29.53
    """
    i = max(0, min(100, int(illum)))
    try:
        d = datetime.strptime(date_iso, "%Y-%m-%d").date()
        y = (d - timedelta(days=1)).isoformat()
        _, yill = get_moon_phase(y)
        waxing = i >= int(yill)
    except Exception:
        waxing = None

    if waxing is True:
        return (i / 100.0) * ANCHORS["F"]
    if waxing is False:
        return ANCHORS["F"] + (1.0 - i / 100.0) * (T_LUNAR - ANCHORS["F"])
    return (i / 100.0) * T_LUNAR  # fallback tuyến tính

# ==== Mapping: preset & micro =====================================
def map_preset(illum: int, waxing: Optional[bool]) -> Tuple[str, Dict[str, Any]]:
    """
    P1: 0–25
    P3: 75–100
    P2: 25–75 & waxing
    P4: 25–75 & waning
    """
    i = max(0, min(100, int(illum)))
    if i <= 25:
        return "P1", PRESETS["P1"]
    if i >= 75:
        return "P3", PRESETS["P3"]
    if waxing is True:
        return "P2", PRESETS["P2"]
    if waxing is False:
        return "P4", PRESETS["P4"]
    return "P2", PRESETS["P2"]  # không rõ hướng → tạm coi waxing

def map_micro_phase(age: float) -> Tuple[str, Tuple[float, float]]:
    """
    Gán tuổi trăng vào khoảng giữa 2 anchor liên tiếp (8 micro-phase).
    """
    keys = list(ANCHORS.keys())
    vals = list(ANCHORS.values())
    for i in range(len(keys)):
        a1, a2 = vals[i], vals[(i + 1) % len(vals)]
        if a1 <= age < a2 or (i == len(keys) - 1 and (age >= a1 or age < a2)):
            return keys[i], (a1, a2)
    return "N", (0.0, T_LUNAR)

# ==== Public: Moon context block (để formatter in đẹp) ============
def moon_context_v2(phase: str, illum: int, date_iso: str) -> Dict[str, Any]:
    illum = int(illum)
    age = estimate_age(illum, date_iso)

    # xác định waxing/waning
    try:
        d = datetime.strptime(date_iso, "%Y-%m-%d").date()
        y = (d - timedelta(days=1)).isoformat()
        _, yill = get_moon_phase(y)
        waxing = illum >= int(yill)
    except Exception:
        waxing = None

    # preset
    pid, preset_info = map_preset(illum, waxing)
    low_pct, high_pct = preset_info["range"]
    progress_pct = 0 if high_pct == low_pct else int(round(max(0.0, min(1.0, (illum - low_pct) / (high_pct - low_pct))) * 100))

    # micro 8
    micro_key, (low_age, high_age) = map_micro_phase(age)
    macro_key, _ = _nearest_main_anchor(age)
    macro_name = ANCHOR_LABEL.get(macro_key, macro_key)

    # stage vs mốc chính
    sign = _signed_circ_diff(age, ANCHORS[macro_key])
    if abs(sign) < 0.25:
        stage = "on anchor"
    elif sign < 0:
        stage = "pre"
    else:
        stage = "post"

    return {
        "preset": f"{pid}: {preset_info['label']}",
        "preset_id": pid,
        "preset_range": (low_pct, illum, high_pct),  # (%)
        "micro_phase": {
            "key": micro_key,
            "label": {
                "N": "New Moon", "WC": "Waxing Crescent", "FQ": "First Quarter",
                "WG": "Waxing Gibbous", "F": "Full Moon", "Wg": "Waning Gibbous",
                "LQ": "Last Quarter", "Wc": "Waning Crescent"
            }.get(micro_key, micro_key)
        },
        "micro_age_range": (round(low_age, 2), round(age, 2), round(high_age, 2)),
        "progress_pct": f"{progress_pct}%",
        "progress_stage": f"{stage} of {macro_name}",
        "suggestions": list(preset_info.get("suggestions", [])),
    }

def next_anchor_dates(date_iso: str) -> Dict[str, str]:
    """
    Ước lượng ngày JST cho 4 mốc sắp tới (🌑/🌓/🌕/🌗) dựa trên tuổi trăng xấp xỉ.
    """
    _, illum = get_moon_phase(date_iso)
    age = estimate_age(int(illum), date_iso)
    base_dt = TOKYO_TZ.localize(datetime.strptime(date_iso, "%Y-%m-%d"))
    out: Dict[str, str] = {}
    for k in MAIN_ANCHORS:
        A = ANCHORS[k]
        delta = _forward_delta_days(age, A)
        dt = base_dt + timedelta(days=delta)
        out[k] = dt.date().isoformat()
    return out

# ==== Unified Moon scoring for TFs ================================
def moon_signed_score_for_tf(phase: str, illum: int, date_iso: str, tf: str = "H4") -> Dict[str, Any]:
    """
    Trả về bộ điểm THỐNG NHẤT cho H4/M30 (không in ra block Moon context).
    Output:
      {
        "tf": "H4"|"M30",
        "signed": float,     # điểm có dấu: dương ủng hộ LONG, âm ủng hộ SHORT (khoảng -2..+2)
        "abs": float,        # độ mạnh tuyệt đối 0..2
        "bias": "LONG"|"SHORT"|"NEUTRAL",
        "tag": "preset=P4, illum=59%, pre of Last Quarter"
      }
    """
    date_iso = str(date_iso)
    illum = int(illum)
    mctx = moon_context_v2(phase, illum, date_iso)  # dùng preset_id + stage

    pid = mctx["preset_id"]     # P1..P4
    stage = mctx["progress_stage"]  # "pre of X" | "post of X" | "on anchor of X"
    stage_key = "pre" if stage.startswith("pre") else ("post" if stage.startswith("post") else ("on" if stage.startswith("on") else "none"))

    # --- Base signed score theo preset & stage ---
    signed = 0.0
    if pid == "P1":
        if stage_key == "pre":    signed = -1.5  # short tới New
        elif stage_key == "on":   signed = +1.5  # đảo lên từ New
        elif stage_key == "post": signed = +1.0
    elif pid == "P2":
        if stage_key == "pre":    signed = +1.0
        elif stage_key == "on":   signed = +0.5  # quanh FQ dễ side
        elif stage_key == "post": signed = +1.0
    elif pid == "P3":
        if stage_key == "pre":    signed = +1.0
        elif stage_key == "on":   signed = -1.5  # Full → cẩn thận đảo
        elif stage_key == "post": signed = -1.0
    elif pid == "P4":
        if stage_key == "pre":    signed = -1.0
        elif stage_key == "on":   signed = -0.5  # quanh LQ hay side
        elif stage_key == "post": signed = -1.0

    # --- Scale theo TF
    tf = (tf or "H4").upper()
    scale = 1.0 if tf == "H4" else 0.8
    signed *= scale

    # Clamp & derive bias
    if signed > 0.25:  bias = "LONG"
    elif signed < -0.25: bias = "SHORT"
    else: bias = "NEUTRAL"

    out = {
        "tf": tf,
        "signed": round(signed, 2),
        "abs": round(abs(signed), 2),
        "bias": bias,
        "tag": f"preset={pid}, illum={illum}%, {stage}",
    }
    return out

# ==== Compact bonus (để in 1 dòng trong /report) ==================
def moon_bonus_for_report(date_iso: str) -> Tuple[float, str]:
    """
    (Legacy – vẫn giữ tương thích)
    Trả về (bonus_float, tag_text) phục vụ dòng:
    'Moon bonus: {bonus} ({tag})'
    → Bonus = |signed score của H4|, giới hạn 0..1.5 cho cân đối tổng điểm.
    """
    phase, illum = get_moon_phase(date_iso)
    s = moon_signed_score_for_tf(phase, illum, date_iso, tf="H4")
    bonus = min(1.5, max(0.0, float(abs(s.get("signed", 0.0)))))  # 0..1.5
    return round(bonus, 2), f"{s['tag']}"

# ===================== P1–P4 resolver (cho /preset auto) =====================
def _today_jst_iso() -> str:
    """Lấy ngày hiện tại theo JST (Tokyo) dạng YYYY-MM-DD."""
    return datetime.now(TOKYO_TZ).date().isoformat()

def _phase_direction_by_yesterday(date_iso: str, illum_today: int) -> str:
    """Xác định waxing/waning bằng cách so illum hôm qua."""
    try:
        d = datetime.strptime(date_iso, "%Y-%m-%d").date()
        y = (d - timedelta(days=1)).isoformat()
        _, yill = get_moon_phase(y)
        return "waxing" if illum_today >= int(yill) else "waning"
    except Exception:
        return "unknown"

def resolve_preset_code(date_iso: Optional[str] = None) -> Tuple[str, Dict[str, Any]]:
    """
    Trả về (pcode, meta) với pcode ∈ {'P1','P2','P3','P4'} theo %illum + hướng (waxing/waning).
    meta gồm: {label, suggestions, range, direction, illum, phase}
    """
    day = date_iso or _today_jst_iso()
    phase, illum = get_moon_phase(day)
    try:
        illum = int(illum)
    except Exception:
        illum = int(float(illum or 0))

    direction = _phase_direction_by_yesterday(day, illum)

    # chọn P-code theo % + hướng
    if 0 <= illum < 25:
        pcode = "P1"
    elif 25 <= illum < 75:
        pcode = "P2" if direction == "waxing" else "P4"
    else:
        pcode = "P3"

    meta = PRESETS[pcode].copy()
    meta.update({"direction": direction, "illum": illum, "phase": phase})
    return pcode, meta

def moon_bonus_for_report_v2(date_iso: Optional[str] = None) -> Tuple[float, str]:
    """
    Bonus theo preset (P1..P4) để cộng vào Total = H4 + M30 + moon_bonus.
    Nếu bạn muốn set số liệu khác, đổi bảng BONUS_MAP bên dưới.
    """
    BONUS_MAP = {"P1": 0.8, "P2": 1.2, "P3": 1.0, "P4": 1.2}
    day = date_iso or _today_jst_iso()
    pcode, meta = resolve_preset_code(day)
    lo, hi = meta["range"]
    bonus = float(BONUS_MAP.get(pcode, 1.0))
    tag = f"{pcode} ({meta['direction']} {lo}–{hi}%) — {meta['label']}"
    return bonus, tag
