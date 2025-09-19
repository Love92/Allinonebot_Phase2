# ----------------------- data/market_data.py -----------------------
from __future__ import annotations
import requests, pandas as pd
from typing import Optional
import pytz
from datetime import datetime

VN_TZ = pytz.timezone("Asia/Ho_Chi_Minh")

def get_klines(symbol="BTCUSDT", interval="30m", limit=200) -> Optional[pd.DataFrame]:
    url = "https://api.binance.com/api/v3/klines" # Spot, adequate for analysis
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        df = pd.DataFrame(data, columns=[
            "open_time","open","high","low","close","volume","close_time","quote_asset_volume",
            "number_of_trades","taker_buy_base","taker_buy_quote","ignore"
        ])
        df["close_time"] = pd.to_datetime(df["close_time"], unit="ms").dt.tz_localize("UTC").dt.tz_convert(VN_TZ)
        for col in ["open","high","low","close","volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        return df
    except Exception as e:
        print("get_klines error:", e)
        return None