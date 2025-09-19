# -*- coding: utf-8 -*-
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Tuple, Dict
import os
import pandas as pd
import numpy as np
import ta

# get_klines: ưu tiên data.market_data, fallback root.market_data
try:
    from data.market_data import get_klines
except Exception:
    from market_data import get_klines


# ======================= Helpers =======================
def _rsi_zone(v: float) -> str:
    if v is None or (isinstance(v, float) and np.isnan(v)): return "Z?"
    if v < 30:  return "Z1"
    if v < 45:  return "Z2"
    if v < 55:  return "Z3"
    if v < 70:  return "Z4"
    return "Z5"

def _crossed_up(a_prev, a_now, b_prev, b_now) -> bool:
    return (a_prev is not None and b_prev is not None
            and a_now is not None and b_now is not None
            and a_prev <= b_prev and a_now > b_now)

def _crossed_down(a_prev, a_now, b_prev, b_now) -> bool:
    return (a_prev is not None and b_prev is not None
            and a_now is not None and b_now is not None
            and a_prev >= b_prev and a_now < b_now)

def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()

def _getenv_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, default))
    except Exception:
        return default

def _getenv_int(key: str, default: int) -> int:
    try:
        return int(float(os.getenv(key, default)))
    except Exception:
        return default

def _getenv_bool(key: str, default: bool) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")

def _getenv_str(key: str, default: str) -> str:
    v = os.getenv(key)
    return (v if v is not None else default).strip()


def _compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    RSI(14), EMA(RSI,12), Stoch%D & SlowD, volMA20, wick%.
    ⚠️ df truyền vào đã bỏ nến đang chạy (để khớp TradingView).
    """
    # Ép kiểu số
    for c in ("open", "high", "low", "close", "volume"):
        df[c] = pd.to_numeric(df[c], errors="coerce")

    open_ = df["open"].astype(float)
    high  = df["high"].astype(float)
    low   = df["low"].astype(float)
    close = df["close"].astype(float)
    vol   = df["volume"].astype(float)

    # RSI 14 + EMA(RSI,12)
    rsi = ta.momentum.RSIIndicator(close=close, window=14).rsi()
    rsi_ema = _ema(rsi, 12)

    # Stochastic (giống H4/M30)
    stoch = ta.momentum.StochasticOscillator(
        high=high, low=low, close=close, window=14, smooth_window=3
    )
    stoch_d = stoch.stoch_signal()          # %D (0–100)
    slow_d  = stoch_d.rolling(3).mean()     # SlowD (0–100)

    # MA20 volume kiểu "code cũ": trung bình 20 nến đã đóng gần nhất
    vol_ma20_val = float(vol.tail(20).mean())
    vol_ma20 = pd.Series(vol_ma20_val, index=df.index)

    # wick%
    rng = (high - low).replace(0, np.nan)
    upper_wick = (high - np.maximum(close, open_)) / rng
    lower_wick = (np.minimum(close, open_) - low) / rng

    out = df.copy()
    out["rsi"] = rsi
    out["rsi_ema"] = rsi_ema
    out["stoch_d"] = stoch_d
    out["slow_d"] = slow_d
    out["vol_ma20"] = vol_ma20
    out["uw"] = upper_wick.clip(lower=0).fillna(0.0)
    out["lw"] = lower_wick.clip(lower=0).fillna(0.0)
    return out


@dataclass
class M5Meta:
    close: float
    rsi: float
    rsi_ema: float
    stoch_d: float
    slow_d: float
    vol: float
    volMA20: float
    uw: float
    lw: float
    zone: str

    def as_dict(self) -> Dict[str, float]:
        return {
            "close": self.close, "rsi": self.rsi, "rsi_ema": self.rsi_ema,
            "stoch_d": self.stoch_d, "slow_d": self.slow_d,
            "vol": self.vol, "volMA20": self.volMA20,
            "uw": self.uw, "lw": self.lw, "zone": self.zone,
        }


# ======================= Cluster logic =======================
def _dual_cross_or_alignment(prev_row, row, side: str) -> Tuple[bool, str]:
    """
    “(CROSS & CROSS) OR (ALIGN & ALIGN)” theo side.
    LONG : (RSI cross UP & StochD cross UP)  OR  (RSI>EMA & StochD>SlowD)
    SHORT: (RSI cross DOWN & StochD cross DOWN) OR (RSI<EMA & StochD<SlowD)
    """
    rsi_prev, rsi_now = float(prev_row["rsi"]), float(row["rsi"])
    re_prev, re_now   = float(prev_row["rsi_ema"]), float(row["rsi_ema"])
    d_prev, d_now     = float(prev_row["stoch_d"]), float(row["stoch_d"])
    sd_prev, sd_now   = float(prev_row["slow_d"]), float(row["slow_d"])

    if side == "LONG":
        cross_ok = _crossed_up(rsi_prev, rsi_now, re_prev, re_now) and _crossed_up(d_prev, d_now, sd_prev, sd_now)
        align_ok = (rsi_now > re_now) and (d_now > sd_now)
        return (cross_ok or align_ok), ("dual_cross_ok" if cross_ok else ("dual_align_ok" if align_ok else "no_cross_or_align"))
    else:  # SHORT
        cross_ok = _crossed_down(rsi_prev, rsi_now, re_prev, re_now) and _crossed_down(d_prev, d_now, sd_prev, sd_now)
        align_ok = (rsi_now < re_now) and (d_now < sd_now)
        return (cross_ok or align_ok), ("dual_cross_ok" if cross_ok else ("dual_align_ok" if align_ok else "no_cross_or_align"))


def _scan_cluster_A(df: pd.DataFrame, lookback: int, wick_pct: float, vol_mult: float,
                    desired_side: Optional[str]) -> Tuple[bool, Optional[int], Optional[str], str]:
    """
    Cụm A — Candle+Volume + Zone cực trị:
      - wick (upper/lower) >= wick_pct
      - volume >= vol_mult * vol_ma20
      - zone bắt buộc: Z1 (LONG) / Z5 (SHORT)
      - hướng: khớp desired_side nếu có; nếu không có → suy theo wick & zone.
    """
    end = len(df)
    start = max(0, end - lookback)
    found, idx_best, dir_best, reason = False, None, None, "no_clusterA"

    for i in range(start, end):
        row = df.iloc[i]
        rsi_now = float(row["rsi"])
        zone = _rsi_zone(rsi_now)
        vol = float(row["volume"])
        vma = float(row.get("vol_ma20", 0.0))
        if vma <= 0:
            continue
        if vol < vol_mult * vma:
            continue

        uw = float(row["uw"]); lw = float(row["lw"])
        if zone == "Z1" and lw >= wick_pct:
            dir_guess = "LONG"
        elif zone == "Z5" and uw >= wick_pct:
            dir_guess = "SHORT"
        else:
            continue  # A yêu cầu Z1/Z5

        if desired_side and dir_guess != desired_side.upper():
            continue

        found, idx_best, dir_best, reason = True, i, dir_guess, f"A:{dir_guess}(zone={zone},wick≥{wick_pct},vol≥{vol_mult}×MA20)"

    return found, idx_best, dir_best, reason


# [UNIFY-H4/M30]
#   Cụm B được chỉnh để BỎ ép zone (require_zone) — giống đúng tinh thần H4/M30:
#   chỉ cần (dual CROSS) hoặc (dual ALIGN) cùng hướng. Zone chỉ là thông tin tham khảo,
#   KHÔNG phải điều kiện bắt buộc.
def _scan_cluster_B(df: pd.DataFrame, lookback: int,
                    desired_side: Optional[str]) -> Tuple[bool, Optional[int], Optional[str], str]:
    """
    Cụm B — RSI & EMA(RSI) AND Stoch D & SlowD (đồng bộ H4/M30):
      - (dual CROSS) hoặc (dual ALIGN) cùng hướng.
      - KHÔNG yêu cầu RSI phải ở Z1/Z2 (LONG) hay Z4/Z5 (SHORT).  # [UNIFY-H4/M30]
      - Hướng phải khớp desired_side (nếu có).
    """
    end = len(df)
    start = max(0, end - lookback)
    found, idx_best, dir_best, reason = False, None, None, "no_clusterB"

    for i in range(max(start + 1, 1), end):  # cần i-1
        prev_row = df.iloc[i - 1]
        row = df.iloc[i]

        ok_long, tag_long = _dual_cross_or_alignment(prev_row, row, "LONG")
        ok_short, tag_short = _dual_cross_or_alignment(prev_row, row, "SHORT")

        dir_guess = None
        tag = "no_cross_or_align"
        if ok_long and not ok_short:
            dir_guess, tag = "LONG", tag_long
        elif ok_short and not ok_long:
            dir_guess, tag = "SHORT", tag_short
        else:
            continue  # không rõ hướng

        if desired_side and dir_guess != desired_side.upper():
            continue

        # KHÔNG ép zone ở cụm B nữa.  # [UNIFY-H4/M30]

        found, idx_best, dir_best, reason = True, i, dir_guess, f"B:{dir_guess}({tag})"

    return found, idx_best, dir_best, reason


# ======================= Public API =======================
def _load_df(symbol: str) -> Optional[pd.DataFrame]:
    """
    Lấy dữ liệu 5m, BỎ cây nến đang chạy → chỉ dùng các nến đã đóng (khớp TradingView).
    """
    df = get_klines(symbol=symbol.replace("/", ""), interval="5m", limit=120)
    if df is None or len(df) < 40:
        return None

    # Ép kiểu số trước khi cắt
    for c in ("open", "high", "low", "close", "volume"):
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # BỎ cây nến M5 chưa đóng
    if len(df) > 0:
        df = df.iloc[:-1]

    # Tính chỉ báo & giữ index số
    df = _compute_indicators(df).dropna().reset_index(drop=True)
    if len(df) < 5:
        return None
    return df

def _meta_from_row(row_now) -> M5Meta:
    return M5Meta(
        close=float(row_now["close"]),
        rsi=float(row_now["rsi"]),
        rsi_ema=float(row_now["rsi_ema"]),
        stoch_d=float(row_now["stoch_d"]),
        slow_d=float(row_now["slow_d"]),
        vol=float(row_now["volume"]),
        volMA20=float(row_now["vol_ma20"]),
        uw=float(row_now["uw"]),
        lw=float(row_now["lw"]),
        zone=_rsi_zone(float(row_now["rsi"])),
    )

def _dir_txt(a_now: float, b_now: float) -> str:
    if a_now > b_now: return "up"
    if a_now < b_now: return "down"
    return "—"


def m5_entry_check(symbol: str, desired_side: Optional[str], strict: bool = False) -> Tuple[bool, str, Dict[str, float]]:
    """
    - RELAX: pass nếu (Cluster A) hoặc (Cluster B) trong lookback riêng (M5_LOOKBACK_RELAX).
      • M5_RELAX_NEED_CURRENT=true -> chỉ chấp nhận nếu A/B xảy ra NGAY nến mới đóng (idx = last).
      • (BỎ ép zone cho cụm B, đồng bộ H4/M30).  # [UNIFY-H4/M30]
    - STRICT: cần (A) và (B) xảy ra tuần tự trong ≤ ENTRY_SEQ_WINDOW_MIN phút, cùng hướng,
      quét bằng lookback riêng (M5_LOOKBACK_STRICT).
    - A: wick≥pct & vol≥k×MA20 & zone Z1/Z5 ; B: dual-cross/align (không ép zone).
    """
    df = _load_df(symbol)
    if df is None:
        return False, "no_data", {}

    # ===== ENV =====
    lookback_relax  = _getenv_int("M5_LOOKBACK_RELAX", 3)
    lookback_strict = _getenv_int("M5_LOOKBACK_STRICT", 6)
    relax_need_curr = _getenv_bool("M5_RELAX_NEED_CURRENT", False)

    wick_pct = _getenv_float("M5_WICK_PCT", 0.50)
    vol_mult_relax  = _getenv_float("M5_VOL_MULT_RELAX", 1.0)
    vol_mult_strict = _getenv_float("M5_VOL_MULT_STRICT", 1.1)
    seq_window_min  = _getenv_int("ENTRY_SEQ_WINDOW_MIN", 30)
    relax_kind = _getenv_str("M5_RELAX_KIND", "either").lower()  # either|rsi_only|candle_only

    last_idx = len(df) - 1

    # ===== Relax scan =====
    if not strict:
        foundA, idxA, dirA, reasonA = _scan_cluster_A(df, lookback_relax, wick_pct, vol_mult_relax, desired_side)
        # [UNIFY-H4/M30]: BỎ require_zone ở cụm B
        foundB, idxB, dirB, reasonB = _scan_cluster_B(df, lookback_relax, desired_side)

        def pick_reason_relax() -> str:
            if relax_kind == "rsi_only":
                return reasonB if foundB else "need_clusterB"
            if relax_kind == "candle_only":
                return reasonA if foundA else "need_clusterA"
            # either: ưu tiên cụm xảy ra muộn hơn
            if foundA and foundB:
                return reasonA if idxA >= idxB else reasonB
            return reasonA if foundA else (reasonB if foundB else "need_A_or_B")

        if relax_kind == "rsi_only":
            ok = foundB
            if relax_need_curr and ok:
                ok = (idxB == last_idx)
        elif relax_kind == "candle_only":
            ok = foundA
            if relax_need_curr and ok:
                ok = (idxA == last_idx)
        else:  # either
            ok = (foundA or foundB)
            if relax_need_curr and ok:
                ok = ((foundA and idxA == last_idx) or (foundB and idxB == last_idx))

        reason = pick_reason_relax()
        meta = _meta_from_row(df.iloc[-1]).as_dict()
        return ok, f"relax_{'ok' if ok else 'no'}:{reason}", meta

    # ===== Strict scan =====
    foundA, idxA, dirA, reasonA = _scan_cluster_A(df, lookback_strict, wick_pct, vol_mult_strict, desired_side)
    # [UNIFY-H4/M30]: BỎ require_zone ở cụm B
    foundB, idxB, dirB, reasonB = _scan_cluster_B(df, lookback_strict, desired_side)

    def _delta_min(i1: int, i2: int) -> int:
        return abs(int(i1) - int(i2)) * 5

    if foundA and foundB:
        # Hướng
        if desired_side:
            side_ok = ((dirA is None or dirA == desired_side.upper())
                       and (dirB is None or dirB == desired_side.upper()))
            side_final = desired_side.upper()
        else:
            side_ok = (dirA is not None and dirB is not None and dirA == dirB)
            side_final = dirA if side_ok else None

        if side_ok:
            dmin = _delta_min(idxA, idxB)
            if dmin <= seq_window_min:
                bars_from_endA = (len(df) - 1 - int(idxA))
                bars_from_endB = (len(df) - 1 - int(idxB))
                meta = _meta_from_row(df.iloc[-1]).as_dict()
                reason = f"strict_ok(A@-{bars_from_endA}bars, B@-{bars_from_endB}bars, side={side_final}, Δ={dmin}m)"
                return True, reason, meta
            reason = f"strict_need_seq(Δ={dmin}m>{seq_window_min}m)"
        else:
            reason = f"strict_dir_mismatch(A={dirA},B={dirB},desired={desired_side})"
    else:
        reason = f"strict_need_{'A' if not foundA else 'B'}"

    meta = _meta_from_row(df.iloc[-1]).as_dict()
    return False, reason, meta


def m5_entry_summary(symbol: str, desired_side: Optional[str]) -> Tuple[str, str]:
    """
    Trả về 2 dòng đã format sẵn:
      - Dòng 1: hiển thị badge riêng cho RELAX và STRICT (OK/NO tách bạch)
      - Dòng 2: meta nhanh (vol/wick/RSI×EMA/Stoch)
    """
    ok_relaxed, reason_relaxed, meta = m5_entry_check(symbol, desired_side, strict=False)
    ok_strict,  reason_strict, _   = m5_entry_check(symbol, desired_side, strict=True)

    # Badge nổi bật cho 2 chế độ
    relax_badge  = "🟢 RELAX OK" if ok_relaxed else "⚪ RELAX"
    strict_badge = "🟢 STRICT OK" if ok_strict else "⚪ STRICT"

    # Lý do giữ nguyên chi tiết để debug
    line1 = (
        f"🔎 M5 Entry — {relax_badge}: {reason_relaxed} | "
        f"{strict_badge}: {reason_strict}"
    )

    # Meta (dùng meta của relaxed để giữ ổn định)
    vol, vma = float(meta.get("vol", 0)), float(meta.get("volMA20", 0))
    uw, lw   = float(meta.get("uw", 0.0)), float(meta.get("lw", 0.0))
    rsi, re  = float(meta.get("rsi", 0.0)), float(meta.get("rsi_ema", 0.0))
    d, sd    = float(meta.get("stoch_d", 0.0)), float(meta.get("slow_d", 0.0))

    line2 = (
        "• M5 meta: "
        f"vol{'>' if vol > vma else '≤'}MA20; "
        f"{'wick≥50%' if ((desired_side or 'LONG').upper()=='LONG' and lw>=0.5) or ((desired_side or 'LONG').upper()=='SHORT' and uw>=0.5) else 'wick<50%'}; "
        f"RSI×EMA:{_dir_txt(rsi, re)}; "
        f"Stoch:{_dir_txt(d, sd)}"
    )
    return line1, line2


def m5_snapshot(symbol: str) -> str:
    """
    Snapshot cho /m5report — dùng NẾN ĐÃ ĐÓNG, cùng cách tính MA20 như code cũ.
    """
    df = _load_df(symbol)
    if df is None or len(df) < 5:
        return "⏱ M5 Snapshot: (không đủ dữ liệu)"

    row = df.iloc[-1]
    zone = _rsi_zone(float(row["rsi"]))
    stoch_dir = "↑" if float(row["stoch_d"]) > float(row["slow_d"]) else "↓" if float(row["stoch_d"]) < float(row["slow_d"]) else "→"
    vol_flag = ">" if float(row["volume"]) > float(row["vol_ma20"] or 0.0) else "≤"

    return (
        "⏱ M5 Snapshot:\n"
        f"• Close: {float(row['close']):.2f}\n"
        f"• RSI: {float(row['rsi']):.1f} (zone {zone}) | EMA(RSI12): {float(row['rsi_ema']):.1f}\n"
        f"• Stoch D: {float(row['stoch_d']):.2f} | SlowD: {float(row['slow_d']):.2f} → Stoch: {stoch_dir}\n"
        f"• Nến: uw={float(row['uw']):.2f} | lw={float(row['lw']):.2f}\n"
        f"• Volume: {vol_flag} MA20\n"
    )
