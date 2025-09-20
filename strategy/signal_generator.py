# ----------------------- strategy/signal_generator.py -----------------------
# Logic (giữ khung cũ, bổ sung tinh gọn theo yêu cầu):
# 1) Ưu tiên CROSS kép (RSI↔EMA & Stoch D↔SlowD cùng chiều) trong 2–3 nến → quyết định side zone-free
# 2) Ưu tiên ALIGN kép (RSI>EMA & D>SlowD hoặc cả hai <) → quyết định side zone-free
# 3) Bonus theo chuyển vùng đối xứng:
#    - Safe retrace:  Z1→Z2 (LONG) | Z5→Z4 (SHORT)
#    - Pivot break:   Z3→Z4 (LONG) | Z3→Z2 (SHORT)
#    - Thrust extreme:Z4→Z5 (LONG) | Z2→Z1 (SHORT)
#    - Ở yên trong 1 zone → tiếp diễn theo bias zone
# 4) Cảnh báo cực trị (Z5/S5 cho LONG, Z1/S1 cho SHORT) → giảm nhẹ điểm (không đổi khung text)
#
# KHUNG HIỂN THỊ / RETURN GIỮ NGUYÊN như bản đang chạy OK.

from __future__ import annotations
from typing import Dict, Tuple, List
import datetime as dt
import os
import time

import requests
import pandas as pd
import pytz
import ta

from data.moon_tide import (
    get_tide_events,           # tide window hôm nay
    moon_bonus_for_report,     # điểm moon thống nhất (H4) + tag
)
from config.settings import TIDE_WINDOW_HOURS  # mặc định lấy từ .env
from strategy.m5_strategy import m5_entry_check, m5_entry_summary

JST = pytz.timezone("Asia/Tokyo")
VN_TZ = pytz.timezone("Asia/Ho_Chi_Minh")

_BINANCE_KLINES = "https://api.binance.com/api/v3/klines"

# ================== Beautify helpers (chỉ đổi hiển thị) ==================
def _beautify_report(s: str) -> str:
    if not isinstance(s, str):
        return s
    s = s.replace("&lt;=", "≤").replace("&gt;=", "≥")
    s = s.replace("&lt;", "＜").replace("&gt;", "＞")
    s = s.replace("<=", "≤").replace(">=", "≥")
    s = s.replace(" EMA34<EMA89", " EMA34＜EMA89") \
         .replace(" EMA34>EMA89", " EMA34＞EMA89") \
         .replace(" Close<EMA34", " Close＜EMA34") \
         .replace(" Close>EMA34", " Close＞EMA34") \
         .replace(" close<EMA34", " close＜EMA34") \
         .replace(" close>EMA34", " close＞EMA34")
    s = s.replace("zone Z1(<30)", "zone Z1 [<30]") \
         .replace("zone Z2(30-45)", "zone Z2 [30–45]") \
         .replace("zone Z3(45-55)", "zone Z3 [45–55]") \
         .replace("zone Z4(55-70)", "zone Z4 [55–70]") \
         .replace("zone Z5(>70)", "zone Z5 [>70]")
    s = s.replace("vol>=MA20", "vol ≥ MA20") \
         .replace("vol<=MA20", "vol ≤ MA20") \
         .replace("wick>=50%", "wick ≥ 50%") \
         .replace("wick<=50%", "wick ≤ 50%")
    return s

# ================== Sonic config (đọc ENV runtime) ==================
def _sonic_mode() -> str:
    # off | weight | veto
    return (os.getenv("SONIC_MODE", "weight") or "weight").strip().lower()

def _sonic_weight() -> float:
    try:
        return float(os.getenv("SONIC_WEIGHT", "1.0"))
    except Exception:
        return 1.0

def _get_env_bool(key: str, default_true: bool = True) -> bool:
    val = os.getenv(key, "true" if default_true else "false").strip().lower()
    return val in ("1","true","yes","y","on")

# ================== Data & indicators ==================
def _get_klines(symbol: str, interval: str, limit: int = 400) -> pd.DataFrame:
    """
    Nới lỏng anti-bot 418 (teapot) bằng User-Agent + retry backoff nhẹ.
    """
    headers = {"User-Agent": "Mozilla/5.0 (TradingBot; +https://binance.com)"}
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    last_ex = None
    for i in range(3):
        try:
            r = requests.get(_BINANCE_KLINES, params=params, timeout=10, headers=headers)
            r.raise_for_status()
            break
        except Exception as e:
            last_ex = e
            time.sleep(0.6 * (i + 1))
    else:
        try:
            params["limit"] = max(150, int(limit * 0.6))
            r = requests.get(_BINANCE_KLINES, params=params, timeout=10, headers=headers)
            r.raise_for_status()
        except Exception:
            raise last_ex or RuntimeError("fetch klines failed")

    cols = [
        "open_time","open","high","low","close","volume","close_time",
        "quote_asset_volume","number_of_trades","taker_buy_base",
        "taker_buy_quote","ignore"
    ]
    df = pd.DataFrame(r.json(), columns=cols)
    for c in ["open","high","low","close","volume","quote_asset_volume","taker_buy_base","taker_buy_quote"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True).dt.tz_convert(VN_TZ)
    return df

def _add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["EMA_34"] = ta.trend.ema_indicator(df["close"], window=34)
    df["EMA_89"] = ta.trend.ema_indicator(df["close"], window=89)
    df["RSI_14"] = ta.momentum.rsi(df["close"], window=14)
    df["EMA_RSI_12"] = ta.trend.ema_indicator(df["RSI_14"], window=12)

    stoch = ta.momentum.StochasticOscillator(
        high=df["high"], low=df["low"], close=df["close"], window=14, smooth_window=3
    )
    df["Stoch_D"] = stoch.stoch_signal()                 # %D
    df["Slow_Stoch_D"] = df["Stoch_D"].rolling(3).mean() # SlowD

    # tiện cho directional scoring (prev)
    df["RSI_14_prev"] = df["RSI_14"].shift(1)
    df["EMA_RSI_12_prev"] = df["EMA_RSI_12"].shift(1)
    df["Stoch_D_prev"] = df["Stoch_D"].shift(1)
    df["Slow_Stoch_D_prev"] = df["Slow_Stoch_D"].shift(1)
    return df

def _sonic_trend(row: pd.Series) -> str:
    if pd.isna(row["EMA_34"]) or pd.isna(row["EMA_89"]):
        return "unknown"
    if row["EMA_34"] > row["EMA_89"] and row["close"] > row["EMA_34"]:
        return "up"
    if row["EMA_34"] < row["EMA_89"] and row["close"] < row["EMA_34"]:
        return "down"
    return "side"

# ================== Zone helpers (RSI & Stoch) ==================
def _zone_of_rsi(v: float) -> str:
    if v < 30: return "Z1"
    if v < 45: return "Z2"
    if v <= 55: return "Z3"
    if v <= 70: return "Z4"
    return "Z5"

def _zone_of_stoch(v: float) -> str:
    if v < 20: return "S1"
    if v < 40: return "S2"
    if v <= 60: return "S3"
    if v <= 80: return "S4"
    return "S5"

def _dir_zone(prev_zone: str, cur_zone: str, order: List[str]) -> str:
    ip, ic = order.index(prev_zone), order.index(cur_zone)
    if ic > ip: return "up"
    if ic < ip: return "down"
    return "flat"

def _align_rsirma(rsi: float, rsi_ema: float, gap_min: float = 0.0) -> str:
    diff = rsi - rsi_ema
    if diff >= gap_min: return "long"
    if diff <= -gap_min: return "short"
    return "none"

def _align_stoch(d: float, sd: float, gap_min: float) -> str:
    diff = d - sd
    if diff >= gap_min: return "long"
    if diff <= -gap_min: return "short"
    return "none"

def _slope(prev_v: float, cur_v: float, slope_min: float) -> str:
    dv = cur_v - prev_v
    if dv >= slope_min: return "up"
    if dv <= -slope_min: return "down"
    return "flat"

def _stoch_recent_cross(series_d: pd.Series, series_sd: pd.Series, lookback_n: int = 3) -> Tuple[bool, str]:
    """
    Có cross trong N nến gần nhất? Trả về (bool, dir) với dir ∈ {'up','down','none'}
    """
    if len(series_d) < lookback_n + 1 or len(series_sd) < lookback_n + 1:
        return False, "none"
    d = series_d.iloc[-(lookback_n+1):].reset_index(drop=True)
    sd = series_sd.iloc[-(lookback_n+1):].reset_index(drop=True)
    last_dir = "none"
    crossed = False
    for i in range(1, len(d)):
        prev_diff = d.iloc[i-1] - sd.iloc[i-1]
        cur_diff  = d.iloc[i]   - sd.iloc[i]
        if pd.notna(prev_diff) and pd.notna(cur_diff) and (prev_diff * cur_diff <= 0):
            crossed = True
            if cur_diff > 0: last_dir = "up"
            elif cur_diff < 0: last_dir = "down"
    return crossed, last_dir

def _rsi_recent_cross(series_rsi: pd.Series, series_ema: pd.Series, lookback_n: int = 2) -> Tuple[bool, str]:
    """
    Cross RSI vs EMA(RSI) trong N nến gần nhất? (bool, dir)
    """
    if len(series_rsi) < lookback_n + 1 or len(series_ema) < lookback_n + 1:
        return False, "none"
    r = series_rsi.iloc[-(lookback_n+1):].reset_index(drop=True)
    e = series_ema.iloc[-(lookback_n+1):].reset_index(drop=True)
    crossed = False
    last_dir = "none"
    for i in range(1, len(r)):
        prev_diff = r.iloc[i-1] - e.iloc[i-1]
        cur_diff  = r.iloc[i]   - e.iloc[i]
        if pd.notna(prev_diff) and pd.notna(cur_diff) and (prev_diff * cur_diff <= 0):
            crossed = True
            if cur_diff > 0: last_dir = "up"
            elif cur_diff < 0: last_dir = "down"
    return crossed, last_dir

def _zone_transition_bonus(z_prev: str, z_cur: str, side: str,
                           safe_bonus: float, pivot_bonus: float, thrust_bonus: float) -> float:
    """
    Bonus theo chuyển vùng đối xứng (RSI hoặc Stoch – truyền zone tương ứng):
    - Safe:  Z1→Z2 long | Z5→Z4 short
    - Pivot: Z3→Z4 long | Z3→Z2 short
    - Thrust:Z4→Z5 long | Z2→Z1 short
    """
    if side not in ("LONG","SHORT"):
        return 0.0
    # Safe retrace
    if side == "LONG" and z_prev == "Z1" and z_cur == "Z2":
        return safe_bonus
    if side == "SHORT" and z_prev == "Z5" and z_cur == "Z4":
        return safe_bonus
    # Pivot break
    if side == "LONG" and z_prev == "Z3" and z_cur == "Z4":
        return pivot_bonus
    if side == "SHORT" and z_prev == "Z3" and z_cur == "Z2":
        return pivot_bonus
    # Thrust extreme
    if side == "LONG" and z_prev == "Z4" and z_cur == "Z5":
        return thrust_bonus
    if side == "SHORT" and z_prev == "Z2" and z_cur == "Z1":
        return thrust_bonus
    return 0.0

# ================== Directional scoring per TF ==================
def score_rsi_directional(tf: str,
                          prev_rsi: float, cur_rsi: float, rsi_ema: float) -> tuple[float, str, dict]:
    """
    Giữ logic cũ, nới lỏng H4: cho điểm khi Z4 đảo xuống & align_short (trước đây dễ 0).
    """
    is_h4 = (tf.upper() == "H4")
    z_prev, z_cur = _zone_of_rsi(prev_rsi), _zone_of_rsi(cur_rsi)
    move = _dir_zone(z_prev, z_cur, ["Z1","Z2","Z3","Z4","Z5"])

    # Cho RSI có "gap" nhẹ để coi thật sự trên/dưới EMA (lọc nhiễu), default=2.0
    rsi_gap_min = float(os.getenv("RSI_GAP_MIN", "2.0"))
    align = _align_rsirma(cur_rsi, rsi_ema, rsi_gap_min)

    z4_down_short_h4 = float(os.getenv("H4_RSI_Z4_DOWN_SHORT", "1.5"))  # mặc định 1.5 điểm
    z3_align_base_h4 = float(os.getenv("H4_RSI_Z3_ALIGN", "1.0"))       # mặc định 1.0
    z2_down_short_h4 = float(os.getenv("H4_RSI_Z2_DOWN_SHORT", "2.0"))  # mặc định 2.0

    base = 0.0; side = "NONE"
    if z_cur == "Z2":
        if move == "down" and align == "short":
            base, side = (z2_down_short_h4 if is_h4 else 1.5), "SHORT"
        elif move == "up" and align == "long":
            base, side = (1.0 if is_h4 else 1.5), "LONG"
    elif z_cur == "Z4":
        if move == "up" and align == "long":
            base, side = (2.0 if is_h4 else 1.5), "LONG"
        elif move == "down" and align == "short":
            base, side = (z4_down_short_h4 if is_h4 else 1.0), "SHORT"
    elif z_cur == "Z3":
        if align == "long" and move in ("up","flat"):
            base, side = ((z3_align_base_h4 if is_h4 else 1.5), "LONG")
        elif align == "short" and move in ("down","flat"):
            base, side = ((z3_align_base_h4 if is_h4 else 1.5), "SHORT")
        else:
            base, side = -1.0, "NONE"  # barrier nếu không rõ align
    elif z_cur == "Z1":
        if align == "long": base, side = (1.5 if is_h4 else 2.0), "LONG"
    elif z_cur == "Z5":
        if align == "short": base, side = (1.5 if is_h4 else 2.0), "SHORT"

    dbg = {"zone": z_cur, "move": move, "align": align, "base": round(base,2)}
    return round(base,2), side, dbg

def score_stoch_directional(tf: str,
                            prev_d: float, cur_d: float, prev_sd: float, cur_sd: float,
                            series_d: pd.Series, series_sd: pd.Series,
                            gap_min: float, slope_min: float, recent_n: int) -> tuple[float, str, dict]:
    """
    Giữ logic cũ (nới S3: slope + align cùng hướng → + điểm nhẹ).
    """
    is_h4 = (tf.upper() == "H4")
    z_prev, z_cur = _zone_of_stoch(prev_d), _zone_of_stoch(cur_d)
    move = _dir_zone(z_prev, z_cur, ["S1","S2","S3","S4","S5"])
    align = _align_stoch(cur_d, cur_sd, gap_min)
    slope_dir = _slope(prev_d, cur_d, slope_min)
    recent_cross, cross_dir = _stoch_recent_cross(series_d, series_sd, recent_n)

    s3_slope_bonus_h4 = float(os.getenv("H4_STCH_S3_SLOPE_BONUS", "0.5"))   # +0.5
    s3_slope_bonus_m30 = float(os.getenv("M30_STCH_S3_SLOPE_BONUS", "1.0")) # +1.0

    base = 0.0; side = "NONE"
    if z_cur == "S2":
        if move == "down" and align == "short": base, side = (1.5 if is_h4 else 2.0), "SHORT"
        elif move == "up"   and align == "long": base, side = (1.0 if is_h4 else 1.5), "LONG"
    elif z_cur == "S4":
        if move == "up"   and align == "long":  base, side = (1.5 if is_h4 else 2.0), "LONG"
        elif move == "down" and align == "short": base, side = (1.0 if is_h4 else 1.5), "SHORT"
    elif z_cur == "S3":
        if recent_cross and align == "long":    base, side = (1.0 if is_h4 else 1.5), "LONG"
        elif recent_cross and align == "short": base, side = (1.0 if is_h4 else 1.5), "SHORT"
        else:
            if align == "short" and slope_dir == "down":
                base, side = ((s3_slope_bonus_h4 if is_h4 else s3_slope_bonus_m30), "SHORT")
            elif align == "long" and slope_dir == "up":
                base, side = ((s3_slope_bonus_h4 if is_h4 else s3_slope_bonus_m30), "LONG")
            else:
                base, side = 0.0, "NONE"
    elif z_cur == "S1":
        if align == "long" and slope_dir == "up": base, side = (1.0 if is_h4 else 1.5), "LONG"
        if align == "long" and recent_cross:      base += 0.5
    elif z_cur == "S5":
        if align == "short" and slope_dir == "down": base, side = (1.0 if is_h4 else 1.5), "SHORT"
        if align == "short" and recent_cross:        base += 0.5

    dbg = {"zone": z_cur, "move": move, "align": align, "slope": slope_dir, "cross": recent_cross, "cross_dir": cross_dir, "base": round(base,2)}
    return round(base,2), side, dbg

def score_tf_directional_v2(tf: str,
                            # RSI
                            prev_rsi: float, cur_rsi: float, rsi_ema: float,
                            # Stoch
                            prev_d: float, cur_d: float, prev_sd: float, cur_sd: float,
                            series_d: pd.Series, series_sd: pd.Series,
                            # NEW: pass series RSI to xét cross gần đây
                            series_rsi: pd.Series = None, series_rsi_ema: pd.Series = None,
                            # Sonic
                            sonic_mode: str = "off", sonic_weight: float = 1.0
                            ) -> tuple[float, str, dict]:

    # ENV knobs
    gap_min_st = float(os.getenv("STCH_GAP_MIN", "3.0"))
    slope_min = float(os.getenv("STCH_SLOPE_MIN", "2.0"))
    recent_n = int(float(os.getenv("STCH_RECENT_N", "3")))
    cross_n = int(float(os.getenv("CROSS_RECENT_N", "2")))
    rsi_gap_min = float(os.getenv("RSI_GAP_MIN", "2.0"))

    # RSI part (base theo logic cũ)
    rsi_score, rsi_side, rsi_dbg = score_rsi_directional(tf, prev_rsi, cur_rsi, rsi_ema)
    # Stoch part (base theo logic cũ)
    st_score, st_side, st_dbg = score_stoch_directional(
        tf, prev_d, cur_d, prev_sd, cur_sd, series_d, series_sd, gap_min_st, slope_min, recent_n
    )

    # ===== Overrides zone-free theo yêu cầu =====
    ovr_dir = None
    ovr_kind = None
    # 1) Dual-cross cùng chiều trong 2–3 nến
    rsi_cross_ok, rsi_cross_dir = (False, "none")
    if series_rsi is not None and series_rsi_ema is not None:
        rsi_cross_ok, rsi_cross_dir = _rsi_recent_cross(series_rsi, series_rsi_ema, cross_n)
    st_cross_ok, st_cross_dir = _stoch_recent_cross(series_d, series_sd, cross_n)

    if _get_env_bool("TF_DUAL_CROSS_OVERRIDE", True) and rsi_cross_ok and st_cross_ok:
        if rsi_cross_dir == "up" and st_cross_dir == "up":
            ovr_dir, ovr_kind = "LONG", "dual_cross"
        elif rsi_cross_dir == "down" and st_cross_dir == "down":
            ovr_dir, ovr_kind = "SHORT", "dual_cross"

    # 2) Dual-align (RSI & Stoch cùng align, qua gap)
    if ovr_dir is None and _get_env_bool("TF_ALIGN_OVERRIDE", True):
        rsi_align = _align_rsirma(cur_rsi, rsi_ema, rsi_gap_min)
        st_align = _align_stoch(cur_d, cur_sd, gap_min_st)
        if rsi_align == "long" and st_align == "long":
            ovr_dir, ovr_kind = "LONG", "dual_align"
        elif rsi_align == "short" and st_align == "short":
            ovr_dir, ovr_kind = "SHORT", "dual_align"

    # Raw score từ hai thành phần
    raw = rsi_score + st_score

    # Chọn side theo thành phần mạnh hơn nếu bất đồng (base rule cũ)
    if rsi_side == st_side:
        side = rsi_side
    else:
        side = rsi_side if abs(rsi_score) >= abs(st_score) else st_side

    # Nếu override kích hoạt → cưỡng chế side & cộng bonus nhẹ
    BONUS_CROSS = float(os.getenv("TF_CROSS_BONUS", "1.5"))
    BONUS_ALIGN = float(os.getenv("TF_ALIGN_BONUS", "1.0"))
    if ovr_dir is not None:
        side = ovr_dir
        raw += (BONUS_CROSS if ovr_kind == "dual_cross" else BONUS_ALIGN)

    # ===== Bonus theo chuyển vùng đối xứng (RSI & Stoch) =====
    if _get_env_bool("ZONE_BONUS_ON", True):
        z_prev_rsi, z_cur_rsi = _zone_of_rsi(prev_rsi), _zone_of_rsi(cur_rsi)
        z_prev_st,  z_cur_st  = _zone_of_stoch(prev_d), _zone_of_stoch(cur_d)
        SAFE_BONUS   = float(os.getenv("ZONE_SAFE_BONUS", "0.6"))
        PIVOT_BONUS  = float(os.getenv("ZONE_PIVOT_BONUS", "1.0"))
        THRUST_BONUS = float(os.getenv("ZONE_THRUST_BONUS", "0.5"))
        raw += _zone_transition_bonus(z_prev_rsi, z_cur_rsi, side, SAFE_BONUS, PIVOT_BONUS, THRUST_BONUS)
        raw += _zone_transition_bonus(z_prev_st.replace("S","Z"), z_cur_st.replace("S","Z"), side, SAFE_BONUS, PIVOT_BONUS, THRUST_BONUS)

    # ===== Cảnh báo cực trị: giảm nhẹ điểm ở Z5/S5 với LONG, Z1/S1 với SHORT =====
    if _get_env_bool("EXTREME_PENALTY_ON", True):
        EXT_PEN = float(os.getenv("TF_EXTREME_PENALTY", "0.5"))
        r_zone = _zone_of_rsi(cur_rsi)
        s_zone = _zone_of_stoch(cur_d)
        if side == "LONG" and (r_zone == "Z5" or s_zone == "S5"):
            raw -= EXT_PEN
        if side == "SHORT" and (r_zone == "Z1" or s_zone == "S1"):
            raw -= EXT_PEN

    # Sonic trend weight (tuỳ chọn, giữ khung cũ)
    w = 0.0
    if _sonic_mode() == "weight" and side in ("LONG", "SHORT"):
        try:
            w = float(os.getenv("SONIC_WEIGHT", str(sonic_weight)))
        except Exception:
            w = sonic_weight
        raw += w

    score = round(raw, 2)
    dbg = {"RSI": rsi_dbg, "STOCH": st_dbg, "sonic_w": w, "side": side, "score": score,
           "ovr": {"kind": ovr_kind or "", "dir": ovr_dir or "", "rsi_cross": rsi_cross_dir, "st_cross": st_cross_dir}}
    return score, side, dbg

# ================== Tide window ==================
def _parse_tide_events_today() -> List[dt.datetime]:
    today_vn = dt.datetime.now(VN_TZ).date()
    events = get_tide_events(today_vn.strftime("%Y-%m-%d")) or []
    times: List[dt.datetime] = []
    for line in events:
        parts = line.split()
        if len(parts) >= 2 and ":" in parts[1]:
            try:
                hh, mm = parts[1].split(":")
                t = dt.datetime(today_vn.year, today_vn.month, today_vn.day, int(hh), int(mm), tzinfo=VN_TZ)
                times.append(t)
            except Exception:
                continue
    return times

def tide_window_now(now_vn: dt.datetime, hours: float = TIDE_WINDOW_HOURS):
    now_vn = now_vn.astimezone(VN_TZ)
    jst_times = _parse_tide_events_today()
    for t_jst in jst_times:
        t_vn = t_jst.astimezone(VN_TZ)
        start = t_vn - dt.timedelta(hours=hours)
        end   = t_vn + dt.timedelta(hours=hours)
        if start <= now_vn <= end:
            return (start, end)
    return None

# ================== Decision Aggregator (H4 + M30) ==================
def _synergy_bonus(h4_dbg: dict, m30_dbg: dict) -> float:
    """
    Synergy nhẹ (+0.5) khi:
      - H4=SHORT (RSI Z2↓/align_short hoặc side='SHORT') & M30 ở Z4/Z5 với align_short/cross↓
      - H4=LONG  (RSI Z4↑/align_long  hoặc side='LONG')  & M30 ở Z1/Z2 với align_long/cross↑
    Bật/tắt: SYNERGY_ON=true|false
    """
    if (os.getenv("SYNERGY_ON", "true").lower() not in ("1","true","yes","on","y")):
        return 0.0

    try:
        h4_side = (h4_dbg or {}).get("side")
        m30_rsi = ((m30_dbg or {}).get("RSI") or {})
        m30_st  = ((m30_dbg or {}).get("STOCH") or {})
        m30_rsi_zone = m30_rsi.get("zone", "")
        m30_rsi_align = m30_rsi.get("align", "none")
        m30_st_zone = m30_st.get("zone", "")
        m30_st_align= m30_st.get("align", "none")
        m30_cross = bool(m30_st.get("cross", False))

        if h4_side == "SHORT":
            if (m30_rsi_zone in ("Z4","Z5") and m30_rsi_align == "short") or \
               (m30_st_zone  in ("S4","S5") and (m30_st_align == "short" or m30_cross)):
                return 0.5
        if h4_side == "LONG":
            if (m30_rsi_zone in ("Z1","Z2") and m30_rsi_align == "long") or \
               (m30_st_zone  in ("S1","S2") and (m30_st_align == "long" or m30_cross)):
                return 0.5
    except Exception:
        return 0.0
    return 0.0

def _near_align_ok(h4_score: float, h4_side: str, m30_score: float, m30_side: str) -> bool:
    """
    Near-align (không cần đồng pha cứng) nếu:
      - tổng tối thiểu HTF_MIN_ALIGN_SCORE (default 6.5)
      - khoảng cách điểm H4 vs M30 ≤ HTF_NEAR_ALIGN_GAP (default 2.0)
      - không đối nghịch "rõ rệt": LONG vs SHORT đồng thời với điểm mạnh (>2.0 mỗi TF)
    Bật/tắt qua ENV: HTF_NEAR_ALIGN=true|false
    """
    if (os.getenv("HTF_NEAR_ALIGN", "true").lower() not in ("1","true","yes","on","y")):
        return False
    try:
        min_total = float(os.getenv("HTF_MIN_ALIGN_SCORE", "6.5"))
        gap = float(os.getenv("HTF_NEAR_ALIGN_GAP", "2.0"))
    except Exception:
        min_total, gap = 6.5, 2.0

    total = h4_score + m30_score
    if total < min_total:
        return False

    if abs(h4_score - m30_score) > gap:
        return False

    if h4_side == "LONG" and m30_side == "SHORT" and h4_score > 2.0 and m30_score > 2.0:
        return False
    if h4_side == "SHORT" and m30_side == "LONG" and h4_score > 2.0 and m30_score > 2.0:
        return False

    return True

# ================== Public ==================
def evaluate_signal(symbol: str = "BTCUSDT", tide_window_hours: float = TIDE_WINDOW_HOURS) -> Dict:
    try:
        # 0) Tide window?
        now_vn = dt.datetime.now(VN_TZ)
        tide_win = tide_window_now(now_vn, hours=float(tide_window_hours))
        if tide_win is None:
            return {"ok": True, "skip": True, "symbol": symbol, "signal": "NONE", "confidence": 0,
                    "text": f"Ngoài khung thủy triều ±{float(tide_window_hours):.1f}h → tạm quan sát.", "frames": {}}

        # 1) Data + indicators
        df_h4  = _add_indicators(_get_klines(symbol, "4h", 300)).dropna().reset_index(drop=True)
        df_m30 = _add_indicators(_get_klines(symbol, "30m", 400)).dropna().reset_index(drop=True)
        if len(df_h4) < 5 or len(df_m30) < 5:
            return {"ok": False, "skip": True, "symbol": symbol, "signal": "NONE", "confidence": 0, "text": "Dữ liệu chưa đủ.", "frames": {}}

        h4_now, h4_prev = df_h4.iloc[-1], df_h4.iloc[-2]
        m30_now, m30_prev = df_m30.iloc[-1], df_m30.iloc[-2]

        # 1b) Sonic trend (để hiển thị)
        sonic_h4  = _sonic_trend(h4_now)
        sonic_m30 = _sonic_trend(m30_now)

        # 2) Directional scoring per TF
        sc_h4, side_h4, dbg_h4 = score_tf_directional_v2(
            "H4",
            h4_prev["RSI_14"], h4_now["RSI_14"], h4_now["EMA_RSI_12"],
            h4_prev["Stoch_D"], h4_now["Stoch_D"], h4_prev["Slow_Stoch_D"], h4_now["Slow_Stoch_D"],
            df_h4["Stoch_D"], df_h4["Slow_Stoch_D"],
            series_rsi=df_h4["RSI_14"], series_rsi_ema=df_h4["EMA_RSI_12"],
            sonic_mode=_sonic_mode(), sonic_weight=_sonic_weight()
        )
        sc_m30, side_m30, dbg_m30 = score_tf_directional_v2(
            "M30",
            m30_prev["RSI_14"], m30_now["RSI_14"], m30_now["EMA_RSI_12"],
            m30_prev["Stoch_D"], m30_now["Stoch_D"], m30_prev["Slow_Stoch_D"], m30_now["Slow_Stoch_D"],
            df_m30["Stoch_D"], df_m30["Slow_Stoch_D"],
            series_rsi=df_m30["RSI_14"], series_rsi_ema=df_m30["EMA_RSI_12"],
            sonic_mode=_sonic_mode(), sonic_weight=_sonic_weight()
        )

        # 3) Sonic veto (nếu bật)
        signal = "NONE"; skip = True
        if _sonic_mode() == "veto":
            if side_h4 == "LONG" and sonic_h4 == "down":
                side_h4 = "NONE"; sc_h4 = max(0.0, sc_h4 - 1.0)
            if side_h4 == "SHORT" and sonic_h4 == "up":
                side_h4 = "NONE"; sc_h4 = max(0.0, sc_h4 - 1.0)

        # 4) Moon bonus (H4)
        today_jst = dt.datetime.now(JST).strftime("%Y-%m-%d")
        moon_bonus, moon_tag = moon_bonus_for_report(today_jst)   # bonus: 0..1.5, tag chuẩn hoá

        # 5) Optional synergy bonus
        syn = _synergy_bonus(dbg_h4, dbg_m30)

        # 6) Total & decide desired side
        total_raw = sc_h4 + sc_m30 + moon_bonus + syn

        desired = None
        if side_h4 in ("LONG","SHORT"):
            if side_m30 == "NONE" or side_m30 == side_h4:
                desired = side_h4
            else:
                if _near_align_ok(sc_h4, side_h4, sc_m30, side_m30):
                    desired = side_h4 if abs(sc_h4) >= abs(sc_m30) else side_m30
        if desired is None and side_m30 in ("LONG","SHORT"):
            min_m30_takeover = float(os.getenv("M30_TAKEOVER_MIN", "6.0"))
            if sc_m30 >= min_m30_takeover:
                desired = side_m30

        signal = desired if desired in ("LONG","SHORT") else "NONE"
        skip = (signal == "NONE")

        # ===== NEW: Extreme guard (block theo yêu cầu) =====
        guard_note = ""
        if not skip and signal in ("LONG","SHORT"):
            if _get_env_bool("EXTREME_BLOCK_ON", True):
                try:
                    rsi_ob = float(os.getenv("EXTREME_RSI_OB", "70"))
                    rsi_os = float(os.getenv("EXTREME_RSI_OS", "30"))
                    st_ob  = float(os.getenv("EXTREME_STOCH_OB", "80"))
                    st_os  = float(os.getenv("EXTREME_STOCH_OS", "20"))
                except Exception:
                    rsi_ob, rsi_os, st_ob, st_os = 70.0, 30.0, 80.0, 20.0

                # H4 & M30 extremes
                h4_long_block   = (h4_now["RSI_14"] >= rsi_ob) or (h4_now["Stoch_D"] >= st_ob)
                h4_short_block  = (h4_now["RSI_14"] <= rsi_os) or (h4_now["Stoch_D"] <= st_os)
                m30_long_block  = (m30_now["RSI_14"] >= rsi_ob) or (m30_now["Stoch_D"] >= st_ob)
                m30_short_block = (m30_now["RSI_14"] <= rsi_os) or (m30_now["Stoch_D"] <= st_os)

                if signal == "LONG" and (h4_long_block or m30_long_block):
                    skip = True
                    guard_note = "⚠️ Extreme-guard: RSI/Stoch đang <b>quá mua</b> (H4 hoặc M30) → chặn LONG."
                if signal == "SHORT" and (h4_short_block or m30_short_block):
                    skip = True
                    guard_note = "⚠️ Extreme-guard: RSI/Stoch đang <b>quá bán</b> (H4 hoặc M30) → chặn SHORT."

        # 7) M5 entry — đồng bộ format với /m5report
        m5_line, m5_meta = m5_entry_summary(symbol, signal if signal in ("LONG","SHORT") else None)
        m5_ok, m5_reason, m5m = m5_entry_check(symbol, signal if signal in ("LONG","SHORT") else None)
        if not m5_ok and not skip and signal in ("LONG","SHORT"):
            skip = True  # HTF ok nhưng M5 chưa đạt → chờ

        # 8) Format hiển thị
        def _fmt_block(tf: str, now: pd.Series, prev: pd.Series, score: float, side: str, dbg: Dict) -> str:
            rsi_dbg = dbg.get("RSI", {}); st_dbg = dbg.get("STOCH", {})
            st_map = {True: "Cross: ✔", False: "Cross: —"}
            st_dir_txt  = f"move={st_dbg.get('move','?')}, align={st_dbg.get('align','?')}, slope={st_dbg.get('slope','?')}, {st_map.get(bool(st_dbg.get('cross', False)), 'Cross: —')}"
            rsi_dir_txt = f"move={rsi_dbg.get('move','?')}, align={rsi_dbg.get('align','?')}"
            return (
                f"📈 **{tf}** | Score: {score} | Side: {side}\n"
                f"• Close: {now['close']:.2f}\n"
                f"• RSI: {now['RSI_14']:.2f} (zone {_zone_of_rsi(now['RSI_14'])}) | EMA(RSI): {now['EMA_RSI_12']:.2f} → {rsi_dir_txt}\n"
                f"• Stoch D: {now['Stoch_D']:.2f} | SlowD: {now['Slow_Stoch_D']:.2f} (zone {_zone_of_stoch(now['Stoch_D'])}) → {st_dir_txt}\n"
            )

        block_h4  = _fmt_block("H4",  h4_now,  h4_prev,  sc_h4,  side_h4,  dbg_h4)
        block_m30 = _fmt_block("M30", m30_now, m30_prev, sc_m30, side_m30, dbg_m30)

        def _sonic_icon(s: str) -> str:
            return "🟢" if s == "up" else ("🔴" if s == "down" else ("🟡" if s == "side" else "⚪"))

        def _sonic_weight_applied(sonic: str, tf_dbg: dict) -> bool:
            if _sonic_mode() != "weight": return False
            rsi_dbg = (tf_dbg or {}).get("RSI") or {}
            align = rsi_dbg.get("align")
            return (sonic == "up" and align == "long") or (sonic == "down" and align == "short")

        sonic_summary = (
            f"🧲 Sonic R: "
            f"H4={_sonic_icon(sonic_h4)} {sonic_h4} "
            f"({'+' + str(int(_sonic_weight())) if _sonic_weight_applied(sonic_h4, dbg_h4) else '0'})"
            f" | M30={_sonic_icon(sonic_m30)} {sonic_m30} "
            f"({'+' + str(int(_sonic_weight())) if _sonic_weight_applied(sonic_m30, dbg_m30) else '0'})"
            f" | mode={_sonic_mode()}({_sonic_weight():.2f})"
        )

        ema_line = (
            f"EMA34/89 → H4: {h4_now['EMA_34']:.2f}/{h4_now['EMA_89']:.2f} | "
            f"M30: {m30_now['EMA_34']:.2f}/{m30_now['EMA_89']:.2f}"
        )

        tide_txt = f"🌊 Tide window (VN): {tide_win[0].strftime('%H:%M')} – {tide_win[1].strftime('%H:%M')}"
        decision_txt = {"LONG":"✅ LONG bias (chờ M5)","SHORT":"✅ SHORT bias (chờ M5)","NONE":"⏸ Chưa đủ điều kiện — Quan sát"}[signal]

        total_disp = round(total_raw, 2)
        sonic_mode_line = f"(SONIC_MODE={_sonic_mode()}, SONIC_WEIGHT={_sonic_weight():.2f})"
        st_env_line = f"(STCH_GAP_MIN={os.getenv('STCH_GAP_MIN','3.0')}, STCH_SLOPE_MIN={os.getenv('STCH_SLOPE_MIN','2.0')}, STCH_RECENT_N={os.getenv('STCH_RECENT_N','3')})"
        htf_env_line = f"(HTF_NEAR_ALIGN={os.getenv('HTF_NEAR_ALIGN','true')}, HTF_MIN_ALIGN_SCORE={os.getenv('HTF_MIN_ALIGN_SCORE','6.5')}, HTF_NEAR_ALIGN_GAP={os.getenv('HTF_NEAR_ALIGN_GAP','2.0')}, SYNERGY_ON={os.getenv('SYNERGY_ON','true')})"

        # Chèn guard_note nếu có
        guard_line = (guard_note + "\n") if guard_note else ""

        text = (
            "━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 **TÍN HIỆU {symbol} — H4 → M30**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{block_h4}\n"
            f"{block_m30}\n"
            f"{sonic_summary}\n"
            f"{ema_line}\n"
            f"{m5_line}\n"
            f"{m5_meta}\n"
            f"🧮 Score H4/M30: {sc_h4} / {sc_m30} | Moon score(H4): {moon_bonus:.1f} ({moon_tag}) | Synergy={syn:.1f} | Total={total_disp} {sonic_mode_line}\n"
            f"{st_env_line}\n"
            f"{htf_env_line}\n"
            f"{tide_txt}\n"
            f"{guard_line}"
            f"🧭 Kết luận: {decision_txt}\n"
            "━━━━━━━━━━━━━━━━━━━━━━━"
        )

        text = _beautify_report(text)

        frames_payload = {
            "H4": {"score": sc_h4, "side": side_h4, **dbg_h4},
            "M30": {"score": sc_m30, "side": side_m30, **dbg_m30},
            "M5": {"ok": m5_ok, "reason": m5_reason, **(m5m or {})},
        }

        return {"ok": True, "skip": skip, "symbol": symbol, "signal": signal, "confidence": int(total_disp),
                "text": text, "frames": frames_payload}

    except Exception as e:
        return {"ok": False, "skip": True, "symbol": symbol, "signal": "NONE", "confidence": 0,
                "text": f"Lỗi evaluate_signal: {e}", "frames": {}}
