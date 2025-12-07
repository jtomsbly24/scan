#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import yfinance as yf
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import warnings
import time
import random

# ------------------ CONFIG ------------------
CSV_FILE = "/home/ubuntu/nse/nse_stock_list.csv"
DB_FILE = "/home/ubuntu/nse/prices.db"
LOOKBACK_DAYS = 500
warnings.filterwarnings("ignore")

# ------------------ TIMEZONE FIX ------------------
import os
os.environ['TZ'] = 'Asia/Kolkata'
time.tzset()

# ------------------ LOAD TICKERS ------------------
df_tickers = pd.read_csv(CSV_FILE, encoding="utf-8", engine="python")
if 'ticker' not in df_tickers.columns:
    raise ValueError("CSV must contain a 'ticker' column")

tickers = df_tickers['ticker'].dropna().unique().tolist()
print(f"Total tickers to download: {len(tickers)}")

# ------------------ DATABASE SETUP ------------------
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS raw_prices(
    date TEXT,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    ticker TEXT,
    PRIMARY KEY (date, ticker)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS metadata(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tickers_total INTEGER,
    tickers_success INTEGER,
    tickers_failed INTEGER,
    updated_at TEXT
)
""")
conn.commit()

# ------------------ DOWNLOAD FUNCTION ------------------
def safe_download(ticker, start, end, retries=3):
    """Retry yfinance download to handle throttling"""
    for attempt in range(1, retries + 1):
        try:
            df = yf.download(
                ticker,
                start=start,
                end=end,
                progress=False,
                auto_adjust=False
            )
            if not df.empty:
                return df
        except Exception as e:
            print(f"Attempt {attempt} failed for {ticker}: {e}")
        time.sleep(random.uniform(1.0, 3.0))
    return pd.DataFrame()

# ------------------ FETCH DATA ------------------
success = 0
failed = 0

for ticker in tickers:
    try:
        cursor.execute("SELECT MAX(date) FROM raw_prices WHERE ticker=?", (ticker,))
        row = cursor.fetchone()
        last_date = row[0]

        start = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1) if last_date else datetime.today() - timedelta(days=LOOKBACK_DAYS)
        end = datetime.today()

        if start > end:
            print(f"{ticker} up-to-date, skipping.")
            success += 1
            continue

        print(f"Downloading {ticker} ({start.date()} → {end.date()})")
        df = safe_download(ticker, start, end)

        if df.empty:
            print(f"No data for {ticker} today (possibly holiday or throttled).")
            failed += 1
            continue

        df['ticker'] = ticker
        df.reset_index(inplace=True)
        df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')

        cursor.executemany("""
            INSERT OR IGNORE INTO raw_prices(date, open, high, low, close, volume, ticker)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'ticker']].values.tolist())

        conn.commit()
        success += 1
        time.sleep(random.uniform(0.8, 2.5))  # throttle

    except Exception as e:
        print(f"Failed {ticker}: {e}")
        failed += 1
        time.sleep(random.uniform(1.0, 3.0))

# ------------------ SAVE METADATA ------------------
cursor.execute(
    "INSERT INTO metadata(tickers_total, tickers_success, tickers_failed, updated_at) VALUES (?,?,?,?)",
    (len(tickers), success, failed, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
)

conn.commit()
conn.close()

print("\n----------------------------------------")
print(f"Success: {success}")
print(f"Failed: {failed}")
print(f"Database saved: {DB_FILE}")
print("----------------------------------------\n")
