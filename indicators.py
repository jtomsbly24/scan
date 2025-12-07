# indicators.py
import os
import shutil
import logging
import sqlite3
import pandas as pd
import numpy as np
from typing import Optional

# === CONFIG ===
MASTER_DB = "/home/ubuntu/nse/prices.db"
WORKING_DB = "/home/ubuntu/streamlitapp/stocks_working.db"
LOGFILE = "/home/ubuntu/streamlitapp/logs/indicators.log"

os.makedirs(os.path.dirname(LOGFILE), exist_ok=True)
logging.basicConfig(filename=LOGFILE,
                    level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")


# === DB Sync ===
def sync_master_to_working() -> bool:
    if not os.path.exists(MASTER_DB):
        raise FileNotFoundError(f"Master DB missing: {MASTER_DB}")

    if (not os.path.exists(WORKING_DB)) or (os.path.getmtime(MASTER_DB) > os.path.getmtime(WORKING_DB)):
        shutil.copyfile(MASTER_DB, WORKING_DB)
        logging.info("Synced master DB → working DB")
        return True

    return False


# === READ RAW PRICES ===
def read_raw_prices(conn: sqlite3.Connection) -> pd.DataFrame:
    df = pd.read_sql("SELECT date, open, high, low, close, volume, ticker FROM raw_prices",
                     conn, parse_dates=["date"])
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values(["ticker", "date"]).reset_index(drop=True)


# === COMPUTE INDICATORS (FINAL SCHEMA) ===
def compute_indicators_in_memory(df: pd.DataFrame) -> pd.DataFrame:
    out = []
    grouped = df.groupby("ticker", sort=False)

    for ticker, g in grouped:
        g = g.sort_values("date").reset_index(drop=True)
        n = len(g)
        if n == 0:
            continue

        g[["open", "high", "low", "close", "volume"]] = g[["open", "high", "low", "close", "volume"]].apply(
            pd.to_numeric, errors="coerce"
        )

        # Moving averages
        g["ema10"] = g["close"].ewm(span=10).mean()
        g["ema20"] = g["close"].ewm(span=20).mean()
        g["ema50"] = g["close"].ewm(span=50).mean()
        g["ema200"] = g["close"].ewm(span=200).mean()

        g["sma7"] = g["close"].rolling(7).mean()
        g["sma21"] = g["close"].rolling(21).mean()
        g["sma63"] = g["close"].rolling(63).mean()
        g["sma126"] = g["close"].rolling(126).mean()

        # ATR14
        g["prev_close"] = g["close"].shift(1)
        tr = pd.concat([
            (g["high"] - g["low"]).abs(),
            (g["high"] - g["prev_close"]).abs(),
            (g["low"] - g["prev_close"]).abs(),
        ], axis=1).max(axis=1)

        g["atr14"] = tr.rolling(14).mean()

        # Volume
        g["volume_avg20"] = g["volume"].rolling(20).mean()

        last = g.iloc[-1]
        prev = g.iloc[-2] if n > 1 else None

        def pct_change(days):
            return ((last["close"] / g.iloc[n - 1 - days]["close"]) - 1) * 100 if (n - 1 - days) >= 0 else np.nan

        def ratio(days):
            idx = n - 1 - days
            if idx < 0:
                return np.nan
            close_val = g.iloc[idx]["close"]
            sma_val = g.iloc[idx]["sma126"]
            return close_val / sma_val if sma_val and sma_val != 0 else np.nan

        # 252 high/low
        high_252 = g["high"].tail(252).max() if n >= 252 else np.nan
        low_252 = g["low"].tail(252).min() if n >= 252 else np.nan

        # Build final row
        out.append({
            "ticker": ticker,
            "date": last["date"],
            "close": float(last["close"]),
            "volume": float(last["volume"]),
            "previous_volume": float(prev["volume"]) if prev is not None else np.nan,
            "volume_avg20": float(last["volume_avg20"]) if not pd.isna(last["volume_avg20"]) else np.nan,
            "daily_change_pct": pct_change(1),
            "daily_change_rupees": float(last["close"] - prev["close"]) if prev is not None else np.nan,
            "weekly_change_pct": pct_change(5),
            "1m_change_pct": pct_change(21),
            "3m_change_pct": pct_change(63),
            "6m_change_pct": pct_change(126),

            "ema10": last["ema10"],
            "ema20": last["ema20"],
            "ema50": last["ema50"],
            "ema200": last["ema200"],
            "sma7": last["sma7"],
            "sma21": last["sma21"],
            "sma63": last["sma63"],
            "sma126": last["sma126"],

            "TI63": (last["sma7"] / last["sma63"]) if last["sma63"] else np.nan,
            "1M_momentum": (last["close"] / last["sma21"]) if last["sma21"] else np.nan,
            "3M_momentum": (last["close"] / last["sma63"]) if last["sma63"] else np.nan,
            "6M_momentum": (last["close"] / last["sma126"]) if last["sma126"] else np.nan,

            "MDT21": ratio(21),
            "MDT50": ratio(50),

            "atr": last["atr14"],

            "252_days_high": high_252,
            "252_days_low": low_252
        })

    res = pd.DataFrame(out)

    if not res.empty:
        base = res["1m_change_pct"].fillna(res["weekly_change_pct"])
        res["relative_strength"] = base.rank(pct=True) * 100
    else:
        res["relative_strength"] = np.nan

    # Final column order
    ordered = [
        "ticker", "date",
        "close", "volume", "previous_volume","volume_avg20",
        "daily_change_pct", "daily_change_rupees",
        "weekly_change_pct", "1m_change_pct", "3m_change_pct", "6m_change_pct",
        "ema10", "ema20", "ema50", "ema200",
        "sma7", "sma21", "sma63", "sma126",
        "TI63", "1M_momentum", "3M_momentum", "6M_momentum",
        "MDT21", "MDT50",
        "atr",
        "252_days_high", "252_days_low",
        "relative_strength"
    ]  

    return res[ordered]


# === WRITE ===
def write_computed_table(res):
    conn = sqlite3.connect(WORKING_DB)
    res.to_sql("computed", conn, if_exists="replace", index=False)
    conn.close()
    logging.info(f"Computed table updated: {len(res)} rows")


# === ORCHESTRATION ===
def ensure_computed_table():
    sync_master_to_working()
    conn = sqlite3.connect(WORKING_DB)
    df = read_raw_prices(conn)
    conn.close()

    res = compute_indicators_in_memory(df)
    write_computed_table(res)
