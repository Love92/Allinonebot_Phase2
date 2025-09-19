# ----------------------- strategy/indicators.py -----------------------
import pandas as pd
import ta

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["EMA_34"] = ta.trend.ema_indicator(df["close"], window=34)
    df["EMA_89"] = ta.trend.ema_indicator(df["close"], window=89)
    df["RSI_14"] = ta.momentum.rsi(df["close"], window=14)
    df["EMA_RSI_12"] = ta.trend.ema_indicator(df["RSI_14"], window=12)
    stoch = ta.momentum.StochasticOscillator(high=df["high"], low=df["low"], close=df["close"], window=14, smooth_window=3)
    df["Stoch_K"] = stoch.stoch()
    df["Stoch_D"] = stoch.stoch_signal()
    df["Slow_Stoch_D"] = df["Stoch_D"].rolling(3).mean()
    df["MA_20"] = df["close"].rolling(20).mean()
    return df

def latest_pair(df: pd.DataFrame):
    return df.iloc[-1], df.iloc[-2]