# ----------------------- tg/formatter.py -----------------------
from __future__ import annotations
from datetime import datetime, timedelta
import html

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
    # nếu anh dùng MarkdownV2 ở nơi khác, có thể cần escape thêm
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


# ===== [PATCH] Helpers cho Preset Range & Progress (half-scale 0–50–100) =====
def _preset_range_by_stage(pcode: str, stage: str, pr_min: float, pr_max: float):
    """
    Chọn (left, right) để in 'L – Hiện tại x% – R' theo đúng preset + stage:
      P1: pre  -> 25 – x – 0      | post -> 0 – x – 25
      P2: pre  -> 25 – x – 50     | post -> 50 – x – 75
      P3: pre  -> 75 – x – 100    | post -> 100 – x – 75
      P4: pre  -> 75 – x – 50     | post -> 50 – x – 25
    Fallback: nếu preset lạ, dùng min/max theo pre/post.
    """
    p = (pcode or "").upper()
    st = (stage or "").strip().lower()
    is_pre = st.startswith("pre")

    if p == "P1":
        return (25.0, 0.0) if is_pre else (0.0, 25.0)
    if p == "P2":
        return (25.0, 50.0) if is_pre else (50.0, 75.0)
    if p == "P3":
        return (75.0, 100.0) if is_pre else (100.0, 75.0)
    if p == "P4":
        return (75.0, 50.0) if is_pre else (50.0, 25.0)

    # fallback an toàn
    return (float(pr_min), float(pr_max)) if is_pre else (float(pr_max), float(pr_min))


def preset_progress_half_scale(pcode: str, stage: str, illum_pct: float) -> float:
    """
    Tính 'Progress' theo logic half-scale (em recommend):
      - Pre  map vào 0..50
      - Post map vào 50..100
    Trả về giá trị đã clamp [0..100].
    """
    st = (stage or "").strip().lower()
    is_pre = st.startswith("pre")

    # left/right theo preset+stage, nhưng chỉ dùng để tính tỉ lệ cục bộ của nửa-range
    if pcode.upper() == "P1":
        L, R = (25.0, 0.0) if is_pre else (0.0, 25.0)
    elif pcode.upper() == "P2":
        L, R = (25.0, 50.0) if is_pre else (50.0, 75.0)
    elif pcode.upper() == "P3":
        L, R = (75.0, 100.0) if is_pre else (100.0, 75.0)
    elif pcode.upper() == "P4":
        L, R = (75.0, 50.0) if is_pre else (50.0, 25.0)
    else:
        # fallback một nửa quanh anchor 50%
        L, R = (0.0, 50.0) if is_pre else (50.0, 100.0)

    cur = float(illum_pct)
    span = abs(R - L) or 1.0
    if R >= L:
        frac = (cur - L) / span
    else:
        frac = (L - cur) / span
    frac = max(0.0, min(1.0, frac))  # clamp 0..1

    base = 0.0 if is_pre else 50.0
    return max(0.0, min(100.0, round(base + frac * 50.0, 2)))


# === NEW: chỉ sửa hiển thị Progress theo hướng preset_range ===
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
            return "0.0"
        if rp >= lp:
            frac = (cp - lp) / span
        else:
            frac = (lp - cp) / span
        pct = max(0.0, min(1.0, frac)) * 100.0
        # hiển thị 0.0/0.5/1 chữ số thập phân nếu cần
        return f"{pct:.1f}".rstrip("0").rstrip(".")
    except Exception:
        return "?"


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

    # preset range (min/max) — “Hiện tại” luôn dùng illum thực tế
    pr = _pick(m2, "preset_range", "presetRange", default=None)
    pr_min = _pick(m2, "preset_min", "presetMin", default=None)
    pr_max = _pick(m2, "preset_max", "presetMax", default=None)
    if isinstance(pr, (list, tuple)) and len(pr) >= 2:
        pr_min = pr_min if pr_min is not None else pr[0]
        pr_max = pr_max if pr_max is not None else pr[-1]
    pr_min = _num_of(pr_min, "?")
    pr_max = _num_of(pr_max, "?")

    # --- [PATCH] Chọn hướng range theo preset + stage (pre/post) ---
    preset_code = str(_pick(m2, "preset", "preset_code", "presetCode", default="")).upper()
    stage_label = _label_of(_pick(m2, "stage", "stage_label", "stageLabel", "progress_stage", default="?"))
    left_range, right_range = _preset_range_by_stage(preset_code, stage_label, pr_min, pr_max)

    # --- Infer micro-phase từ direction + %illum, override nếu context sai ---
    direction   = str(_pick(m2, "direction", "dir", default="")).lower()

    def _infer_micro_phase(dir_str: str, illum_pct):
        try:
            x = float(illum_pct)
        except Exception:
            return "?"
        d = (dir_str or "").lower()
        # neo đặc biệt
        if x <= 0.0:
            return "New Moon"
        if x >= 99.5:
            return "Full Moon"
        # gần quarter
        if abs(x - 50.0) <= 1.0:
            return "Last Quarter" if d == "waning" else "First Quarter"
        # còn lại: Gibbous (>50) / Crescent (<50) theo hướng
        if d == "waning":
            return "Waning Gibbous" if x > 50.0 else "Waning Crescent"
        if d == "waxing":
            return "Waxing Gibbous" if x > 50.0 else "Waxing Crescent"
        # fallback nếu không rõ hướng
        return "Waning Crescent" if x < 50.0 else "Waning Gibbous"

    micro_phase_ctx = _label_of(_pick(m2, "micro_phase", "microPhase", default="?"))
    micro_phase_infer = _infer_micro_phase(direction, illum_i)

    def _need_override(ctx: str, infer: str, x):
        ctxu = (ctx or "?").strip().lower()
        inferu = (infer or "?").strip().lower()
        # thiếu/không rõ → override
        if ctx in (None, "", "?"):
            return True
        # context nói Quarter nhưng %illum không ~50 → override
        try:
            xf = float(x)
            if "quarter" in ctxu and abs(xf - 50.0) > 1.0:
                return True
        except Exception:
            return True
        # nếu hướng waning/waxing không khớp mô tả
        if direction and direction not in ctxu:
            return True
        # khác loại (crescent/gibbous) so với infer → override
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

    # --- Progress & Stage (half-scale 0–50–100, anchor=50%) ---
    stage = stage_label if stage_label not in (None, "") else "?"
    progress_val = preset_progress_half_scale(preset_code, stage, float(illum_i))

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

    # ép kiểu hiển thị đẹp
    def _i(x):
        try:
            return int(float(x))
        except Exception:
            return x

    raw = (
        f"📅 Ngày: {vn_date}\n"
        f"🌙 Preset: {preset_label}\n"
        f" └─ Preset range (%): {_i(left_range)} – Hiện tại {_i(illum_i)}% – {_i(right_range)}\n"
        f" └─ Micro-phase: {micro_phase}\n"
        f" └─ Micro-age (days): {ma_min} – Hiện tại {ma_cur} – {ma_max}\n"
        f" └─ Progress: {_i(progress_val)} | Stage: {stage}\n"
        f" └─ Suggestion(s):\n{('\n'.join([f'   • {str(s)}' for s in (_pick(m2, 'suggestions', 'suggest', default=[]) or [])]) or '   • (no suggestion)')}\n"
        f"{nxt_str}\n\n"
        f"🌊 Thủy triều trong ngày:{tide_block}\n\n"
        f"{status_line}"
        f"{tw_line}"
    )
    return _safe_html(raw)



# ----------------------- /tg/formatter.py -----------------------
