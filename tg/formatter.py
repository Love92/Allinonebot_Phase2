# ----------------------- tg/formatter.py -----------------------
from __future__ import annotations
from datetime import datetime, timedelta
import html, re

# === Imports robust theo nhiều cấu trúc dự án ===
# moon_tide
try:
    from data.moon_tide import (
        get_moon_phase, get_tide_events,
        moon_context_v2, next_anchor_dates,
        ANCHOR_LABEL, ANCHOR_EMOJI
    )
except Exception:
    from moon_tide import (  # type: ignore
        get_moon_phase, get_tide_events,
        moon_context_v2, next_anchor_dates,
        ANCHOR_LABEL, ANCHOR_EMOJI
    )

# time utils (VN_TZ, TOKYO_TZ)
try:
    from utils.time_utils import VN_TZ, TOKYO_TZ  # type: ignore
except Exception:
    from time_utils import VN_TZ, TOKYO_TZ  # type: ignore

# settings (TIDE_WINDOW_HOURS)
try:
    from config.settings import TIDE_WINDOW_HOURS  # type: ignore
except Exception:
    from settings import TIDE_WINDOW_HOURS  # type: ignore


def format_signal_report(res: dict) -> str:
    """
    Fallback text cho các payload cũ (nếu res đã có 'text' thì trả về luôn).
    Trường hợp /autolog dùng text thô từ engine.
    """
    if isinstance(res, dict) and res.get("text"):
        return res["text"]

    h4 = res.get("h4", {}) or {}
    m5 = res.get("m5", {}) or {}
    moon = h4.get("moon", {}) or {}

    notes = "\n".join([f"• {n}" for n in (h4.get("notes") or [])])
    entry_line = ""
    if m5 and m5.get("ok"):
        snap_close = (m5.get("snapshot", {}) or {}).get("close", "?")
        entry_line = f"\nM5 Entry: {'OK' if m5.get('entry_ok') else 'No'} (close={snap_close})"

    return f"""
📌 [ALERT] {h4.get('symbol','?')} — H4 → M30
⏱ H4 signal: {h4.get('direction','?')} — Score: {h4.get('score','?')} / 10
🌙 Moon: {moon.get('phase','?')} ({moon.get('illum','?')}%) → Bias: {moon.get('bias','?')}
🔍 Breakdown:
{notes}
➡ Decision: {(h4.get('decision') or '?').upper()}
{entry_line}
""".strip()


def _parse_jst_times(date_iso: str, events: list[str]) -> list[tuple[str, datetime]]:
    """
    events: ['High 18:20', 'Low 02:00'] (JST)
    Trả về list (label, datetime[JST])
    """
    out: list[tuple[str, datetime]] = []
    y, m, d = [int(x) for x in date_iso.split("-")]
    for e in events:
        parts = e.split()
        if len(parts) >= 2 and ":" in parts[1]:
            hh, mm = parts[1].split(":")
            try:
                t = datetime(y, m, d, int(hh), int(mm), tzinfo=TOKYO_TZ)
                out.append((parts[0].capitalize(), t))
            except Exception:
                continue
    return out


def _tide_status_line(
    vn_now: datetime,
    jst_events: list[tuple[str, datetime]],
    tide_window_hours: float
) -> tuple[str, tuple[datetime, datetime] | None]:
    """
    So sánh now(VN) với ±window quanh các mốc thủy triều (giữ HH:MM VN).
    """
    in_range = []
    for label, t_jst in jst_events:
        hh, mm = t_jst.hour, t_jst.minute
        t_vn = vn_now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        start_vn = t_vn - timedelta(hours=tide_window_hours)
        end_vn   = t_vn + timedelta(hours=tide_window_hours)
        if start_vn <= vn_now <= end_vn:
            in_range.append((abs((vn_now - t_vn).total_seconds()), label, t_vn, start_vn, end_vn))

    if in_range:
        _, label, t_vn, start_vn, end_vn = min(in_range, key=lambda x: x[0])
        line = f"✅ Trong vùng thủy triều (±{tide_window_hours:.1f}h quanh {t_vn.strftime('%H:%M')} {label} tide, giờ VN)"
        return line, (start_vn, end_vn)

    line = f"⏳ Ngoài vùng thủy triều (±{tide_window_hours:.1f}h, giờ VN)"
    return line, None


def _format_next_phases(date_iso: str) -> str:
    """
    ✨ Next phases: 🌑 New 2025-09-22 • 🌓 First 2025-09-29 • 🌕 Full 2025-10-06 • 🌗 Last 2025-10-14
    """
    nxt = next_anchor_dates(date_iso)  # {'N': 'YYYY-MM-DD', 'FQ': ..., 'F': ..., 'LQ': ...}
    parts = []
    for k in ["N", "FQ", "F", "LQ"]:
        label = ANCHOR_LABEL.get(k, k)
        emoji = ANCHOR_EMOJI.get(k, "")
        parts.append(f"{emoji} {label} {nxt.get(k, '?')}")
    return "✨ Next phases: " + " • ".join(parts)


# ---------- Helpers robust cho Moon context ----------
def _label_of(x):
    if isinstance(x, dict):
        return x.get("label") or x.get("name") or x.get("key") or "?"
    return x if isinstance(x, str) else ("?" if x is None else str(x))

def _num_of(x, default="?"):
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return x
        s = str(x).strip().replace("%", "")
        return float(s)
    except Exception:
        return default

def _pick(src: dict, *keys, default=None):
    if not isinstance(src, dict):
        return default
    for k in keys:
        if k in src and src[k] not in (None, ""):
            return src[k]
    return default


def _beautify_report(s: str) -> str:
    """
    Chuẩn hóa các ký hiệu so sánh để KHÔNG bị Telegram HTML hiểu sai.
    Giữ nguyên nội dung anh đang dùng, chỉ thay thế ký tự gây lỗi.
    """
    if not isinstance(s, str):
        return s
    # cả dạng đã escape (&lt;=) và dạng thô (<=)
    s = (s.replace("&lt;=", "≤").replace("&gt;=", "≥")
           .replace("<=", "≤").replace(">=", "≥")
           .replace(" EMA34<EMA89", " EMA34＜EMA89")
           .replace(" EMA34>EMA89", " EMA34＞EMA89")
           .replace(" Close<EMA34", " Close＜EMA34")
           .replace(" Close>EMA34", " Close＞EMA34")
           .replace(" close<EMA34", " close＜EMA34")
           .replace(" close>EMA34", " close＞EMA34"))
    return s


def _safe_html(raw_text: str) -> str:
    """
    1) html.escape để an toàn HTML
    2) _beautify_report để chuyển các so sánh thành kí hiệu/văn tự an toàn
    Thứ tự này cho phép bắt cả '<=' & '&lt;='.
    """
    if raw_text is None:
        return ""
    return _beautify_report(html.escape(str(raw_text), quote=False))


# === NEW (giữ lại): tiến độ theo HƯỚNG range in ra (không dùng để in chính, giữ làm helper) ===
def _directed_progress_from_range(left_range, current_pct, right_range) -> str:
    """
    Tính progress % (0..100) theo HƯỚNG hiển thị của preset_range.
    Ví dụ: left=25, cur=18, right=0  →  (25-18)/25 = 28%
           left=0,  cur=18, right=25 →  (18-0)/25  = 72%
    """
    try:
        lp = float(left_range)
        cp = float(current_pct)
        rp = float(right_range)
        span = abs(rp - lp)
        if span <= 0:
            return "0"
        if rp >= lp:
            frac = (cp - lp) / span
        else:
            frac = (lp - cp) / span
        pct = max(0.0, min(1.0, frac)) * 100.0
        return f"{pct:.0f}"
    except Exception:
        return "0"


# === NEW: xác định HƯỚNG (waxing/waning) ưu tiên dữ liệu stage/direction ===
def _resolve_direction(preset_code: str, stage_label: str, direction_hint: str) -> str:
    """
    Trả về 'waxing' | 'waning'.
    Ưu tiên:
      1) direction_hint từ context (nếu hợp lệ)
      2) SUY LUẬN từ stage quanh 4 anchor (N/FQ/F/LQ)
      3) Mặc định: P2 → waxing; P4 → waning; P1/P3 phụ thuộc stage nếu có, else infer theo %illum ở nơi khác.
    """
    d = (direction_hint or "").strip().lower()
    if d in ("waxing", "waning"):
        return d

    stage = (stage_label or "").strip().lower()
    pc = (preset_code or "").strip().upper()

    # Suy luận theo stage
    if "new moon" in stage:
        if "pre" in stage:
            return "waning"
        if "post" in stage:
            return "waxing"
    if "first quarter" in stage:
        return "waxing"
    if "full moon" in stage:
        if "pre" in stage:
            return "waxing"
        if "post" in stage:
            return "waning"
    if "last quarter" in stage:
        return "waning"

    if pc.startswith("P2"):
        return "waxing"
    if pc.startswith("P4"):
        return "waning"
    return "waxing"


# === NEW: infer micro-phase từ hướng + %illum (khi context không chắc chắn)
def _infer_micro_phase(dir_str: str, illum_pct):
    try:
        x = float(illum_pct)
    except Exception:
        return "?"
    d = (dir_str or "").lower()
    if x <= 0.0:
        return "New Moon"
    if x >= 99.5:
        return "Full Moon"
    if abs(x - 50.0) <= 1.0:
        return "Last Quarter" if d == "waning" else "First Quarter"
    if d == "waning":
        return "Waning Gibbous" if x > 50.0 else "Waning Crescent"
    if d == "waxing":
        return "Waxing Gibbous" if x > 50.0 else "Waxing Crescent"
    return "Waning Crescent" if x < 50.0 else "Waning Gibbous"


# === NEW: Tiến độ 0–100% trong PHẠM VI PRESET (2 nửa mỗi preset, mỗi nửa 50%) ===
def preset_progress_0_100(pcode: str, stage_label: str, illum_pct: float) -> float:
    """
    Mapping:
      P1: pre 25→0 (0–50), post 0→25 (50–100)
      P2: pre 25→50 (0–50), post 50→75 (50–100)          [waxing]
      P3: pre 75→100 (0–50), post 100→75 (50–100)        [pre=waxing, post=waning]
      P4: pre 75→50 (0–50), post 50→25 (50–100)          [waning]
    """
    p = (pcode or "").upper()
    s = (stage_label or "").lower().strip()
    x = float(illum_pct)

    def clamp(v): return max(0.0, min(100.0, v))

    if p == "P1":
        if "pre" in s:   # 25 -> 0
            return clamp(((25.0 - x) / 25.0) * 50.0)
        else:             # 0 -> 25
            return clamp(50.0 + ((x - 0.0) / 25.0) * 50.0)

    if p == "P2":
        if "pre" in s:   # 25 -> 50
            return clamp(((x - 25.0) / 25.0) * 50.0)
        else:             # 50 -> 75
            return clamp(50.0 + ((x - 50.0) / 25.0) * 50.0)

    if p == "P3":
        if "pre" in s:   # 75 -> 100
            return clamp(((x - 75.0) / 25.0) * 50.0)
        else:             # 100 -> 75 (giảm)
            return clamp(50.0 + ((100.0 - x) / 25.0) * 50.0)

    if p == "P4":
        if "pre" in s:   # 75 -> 50 (giảm)
            return clamp(((75.0 - x) / 25.0) * 50.0)
        else:             # 50 -> 25 (giảm)
            return clamp(50.0 + ((50.0 - x) / 25.0) * 50.0)

    # fallback nếu preset lạ
    return 0.0


# === NEW: Chuẩn hoá mã preset để không bị rơi vào mặc định 0 ===
def _normalize_preset_code(raw_code: str, preset_label: str, pr_min, pr_max) -> str:
    """
    Cố gắng suy ra 'P1|P2|P3|P4' từ:
      - raw_code (nếu đã chuẩn)
      - prefix trong preset_label (vd: 'P1: ...')
      - cặp range {min,max}
    """
    code = (raw_code or "").strip().upper()
    if code in ("P1", "P2", "P3", "P4"):
        return code

    m = re.match(r"\s*(P[1-4])\b", str(preset_label or "").strip(), flags=re.I)
    if m:
        return m.group(1).upper()

    try:
        a = float(pr_min)
        b = float(pr_max)
        s = {a, b}
        if s == {0.0, 25.0}:
            return "P1"
        if s == {25.0, 50.0}:
            return "P2"
        if s == {50.0, 75.0}:
            return "P2"  # nửa sau của P2
        if s == {75.0, 100.0}:
            return "P3"
        if s == {50.0, 25.0}:
            return "P4"
        if s == {75.0, 50.0}:
            return "P4"
    except Exception:
        pass
    return ""  # không đoán được


def format_daily_moon_tide_report(vn_date: str, tide_window_hours: float = TIDE_WINDOW_HOURS) -> str:
    # --- Moon (phase + illum) ---
    phase, illum = get_moon_phase(vn_date)
    try:
        illum_i = int(_num_of(illum, 0))
    except Exception:
        illum_i = _num_of(illum, "?")

    m2 = moon_context_v2(
        phase,
        int(illum_i) if isinstance(illum_i, (int, float)) else illum_i,
        vn_date
    ) or {}

    # preset label
    preset_label = _label_of(_pick(m2, "preset_label", "presetName", "preset_name", "preset", default="?"))

    # preset code / direction / stage
    preset_code_raw = str(_pick(m2, "preset", "preset_code", "presetCode", default="")).upper()
    direction_hint = str(_pick(m2, "direction", "dir", default="")).lower()
    stage = _label_of(_pick(m2, "stage", "stage_label", "stageLabel", "progress_stage", default="?"))
    if not stage or stage == "?":
        stage = _label_of(_pick(m2, "stage_text", "stageText", default="?"))

    # preset range (min/max) — “Hiện tại” luôn dùng illum thực tế
    pr = _pick(m2, "preset_range", "presetRange", default=None)
    pr_min = _pick(m2, "preset_min", "presetMin", default=None)
    pr_max = _pick(m2, "preset_max", "presetMax", default=None)
    if isinstance(pr, (list, tuple)) and len(pr) >= 2:
        pr_min = pr_min if pr_min is not None else pr[0]
        pr_max = pr_max if pr_max is not None else pr[-1]
    pr_min = _num_of(pr_min, "?")
    pr_max = _num_of(pr_max, "?")

    # === Quyết định hướng thật sự để IN RANGE cho đúng ===
    resolved_dir = _resolve_direction(preset_code_raw, stage, direction_hint)
    if resolved_dir == "waning":
        left_range, right_range = pr_max, pr_min
    else:
        left_range, right_range = pr_min, pr_max

    # --- Micro-phase: ưu tiên context, nếu lệch thì infer ---
    micro_phase_ctx = _label_of(_pick(m2, "micro_phase", "microPhase", default="?"))
    micro_phase_infer = _infer_micro_phase(resolved_dir, illum_i)

    def _need_override(ctx: str, infer: str, x):
        ctxu = (ctx or "?").strip().lower()
        inferu = (infer or "?").strip().lower()
        if ctx in (None, "", "?"):
            return True
        try:
            xf = float(x)
            if "quarter" in ctxu and abs(xf - 50.0) > 1.0:
                return True
        except Exception:
            return True
        if resolved_dir and resolved_dir not in ctxu:
            return True
        if ("crescent" in inferu and "crescent" not in ctxu) or ("gibbous" in inferu and "gibbous" not in ctxu):
            return True
        return False

    micro_phase = micro_phase_infer if _need_override(micro_phase_ctx, micro_phase_infer, illum_i) else micro_phase_ctx

    # --- Micro-age (min / cur / max) — robust cho dict|list|tuple + alias ---
    ma_min = ma_cur = ma_max = "?"

    mr = _pick(m2, "micro_age_range", "microAgeRange", "age_range", "ageRange", default=None)
    if isinstance(mr, dict):
        ma_min = _pick(mr, "min", "lo", "low", "start", default=ma_min)
        ma_cur = _pick(mr, "cur", "current", "now", "value", "mid", "center", default=ma_cur)
        ma_max = _pick(mr, "max", "hi", "high", "end", default=ma_max)
    elif isinstance(mr, (list, tuple)):
        if len(mr) == 3:
            ma_min, ma_cur, ma_max = mr[0], mr[1], mr[2]
        elif len(mr) >= 2:
            ma_min, ma_max = mr[0], mr[-1]

    # Fallback key rời
    if ma_min == "?":
        ma_min = _pick(m2, "micro_age_min", "microAgeMin", "age_min", "ageMin", default=ma_min)
    if ma_max == "?":
        ma_max = _pick(m2, "micro_age_max", "microAgeMax", "age_max", "ageMax", default=ma_max)
    if ma_cur == "?":
        ma_cur = _pick(
            m2,
            "micro_age_current", "microAgeCurrent", "age_current", "ageCurrent",
            "micro_age_now", "microAgeNow", "age_now", "ageNow",
            "micro_age", "microAge", "age",
            default=ma_cur
        )

    # Chuẩn hóa số
    ma_min = _num_of(ma_min, "?")
    ma_cur = _num_of(ma_cur, "?")
    ma_max = _num_of(ma_max, "?")

    # === Progress: TIẾN ĐỘ 0–100% TRONG PHẠM VI PRESET (theo yêu cầu) ===
    code_norm = _normalize_preset_code(preset_code_raw, preset_label, left_range, right_range)
    if code_norm in ("P1", "P2", "P3", "P4"):
        try:
            preset_prog = preset_progress_0_100(code_norm, stage, float(illum_i))
            progress_str = f"{int(round(preset_prog))}"
        except Exception:
            progress_str = _directed_progress_from_range(left_range, illum_i, right_range)
    else:
        # Nếu vẫn không đoán được preset code → fallback theo hướng hiển thị
        progress_str = _directed_progress_from_range(left_range, illum_i, right_range)

    # --- Tide ---
    tide_lines = get_tide_events(vn_date) or []
    jst_events = _parse_jst_times(vn_date, tide_lines)

    vn_now = datetime.now(VN_TZ)
    status_line, span = _tide_status_line(vn_now, jst_events, tide_window_hours)
    tw_line = ""
    if span:
        start_vn, end_vn = span
        tw_line = f"\n🌊 Tide window (VN): {start_vn.strftime('%H:%M')} – {end_vn.strftime('%H:%M')}"

    # --- Next phases ---
    nxt_str = _format_next_phases(vn_date)

    tide_block = "\n   • " + "\n   • ".join(tide_lines) if tide_lines else ""

    raw = (
        f"📅 Ngày: {vn_date}\n"
        f"🌙 Preset: {preset_label}\n"
        f" └─ Preset range (%): {left_range} – Hiện tại {illum_i}% – {right_range}\n"
        f" └─ Micro-phase: {micro_phase}\n"
        f" └─ Micro-age (days): {ma_min} – Hiện tại {ma_cur} – {ma_max}\n"
        f" └─ Progress: {progress_str} | Stage: {stage}\n"
        f" └─ Suggestion(s):\n"
        f"{('\n'.join([f'   • {str(s)}' for s in (_pick(m2, 'suggestions', 'suggest', default=[]) or [])]) or '   • (no suggestion)')}\n"
        f"{nxt_str}\n\n"
        f"🌊 Thủy triều trong ngày:{tide_block}\n\n"
        f"{status_line}"
        f"{tw_line}"
    )
    return _safe_html(raw)

# ----------------------- /tg/formatter.py -----------------------
