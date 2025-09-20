# ----------------------- data/market_data.py -----------------------
from __future__ import annotations
import requests, pandas as pd
from typing import Optional
import pytz
from datetime import datetime

VN_TZ = pytz.timezone("Asia/Ho_Chi_Minh")

# Dùng 1 session chung + headers để tránh 418/anti-bot
_SESSION = requests.Session()
_SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; TradingBot/1.0; +https://example.local)",
    "Accept": "application/json,text/plain,*/*",
    "Connection": "close",
})

# Fallback nhiều endpoint (Binance có lúc 418/429)
_BINANCE_KLINES_ENDPOINTS = [
    "https://api.binance.com/api/v3/klines",
    "https://api1.binance.com/api/v3/klines",
    "https://api2.binance.com/api/v3/klines",
    "https://api3.binance.com/api/v3/klines",
    "https://data-api.binance.vision/api/v3/klines",  # fallback chính thức
]

def _parse_klines(data) -> pd.DataFrame:
    df = pd.DataFrame(data, columns=[
        "open_time","open","high","low","close","volume","close_time","quote_asset_volume",
        "number_of_trades","taker_buy_base","taker_buy_quote","ignore"
    ])
    # thời gian chốt nến theo VN_TZ để hiển thị/report
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms").dt.tz_localize("UTC").dt.tz_convert(VN_TZ)
    for col in ["open","high","low","close","volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def get_klines(symbol: str = "BTCUSDT", interval: str = "30m", limit: int = 200) -> Optional[pd.DataFrame]:
    params = {"symbol": symbol, "interval": interval, "limit": limit}

    # 1) Thử lần lượt các endpoint
    for url in _BINANCE_KLINES_ENDPOINTS:
        try:
            r = _SESSION.get(url, params=params, timeout=10)
            # Nếu bị 418/429/5xx → thử endpoint khác
            if r.status_code in (418, 429) or (500 <= r.status_code < 600):
                continue
            r.raise_for_status()
            return _parse_klines(r.json())
        except Exception as e:
            # print để debug nhẹ, nhưng không dừng hẳn
            print(f"[get_klines] try {url} error: {e}")
            continue

    # 2) Fallback CCXT (nếu đã cài)
    try:
        import ccxt  # type: ignore
        ex = ccxt.binance({"enableRateLimit": True})
        ohlcv = ex.fetch_ohlcv(symbol, timeframe=interval, limit=limit)
        data = []
        for o in ohlcv:
            # [ts, open, high, low, close, volume]
            data.append([
                o[0], o[1], o[2], o[3], o[4], o[5],
                o[0], 0, 0, 0, 0, 0
            ])
        return _parse_klines(data)
    except Exception as e:
        print(f"[get_klines] ccxt fallback failed: {e}")

    # 3) Bó tay
    return None
