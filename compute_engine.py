# compute_engine.py
import os
import logging
import sqlite3
import pandas as pd
import numpy as np
import ta

MASTER_DB = "/home/ubuntu/nse/prices.db"
WORKING_DB = "/home/ubuntu/streamlitapp/stocks_working.db"
LOGFILE = "/home/ubuntu/streamlitapp/logs/indicators.log"

os.makedirs(os.path.dirname(LOGFILE), exist_ok=True)
logging.basicConfig(filename=LOGFILE,
                    level=logging.ERROR,
                    format="%(asctime)s %(levelname)s %(message)s")


def read_raw_prices(conn):
    df = pd.read_sql("SELECT date, open, high, low, close, volume, ticker FROM raw_prices", conn, parse_dates=['date'])
    df = df.sort_values(['ticker', 'date']).reset_index(drop=True)
    return df


def compute_indicators_for_ticker(df):
    """
    Compute all indicators for one ticker sequentially.
    Returns a single-row DataFrame.
    """
    results = {}
    g = df.sort_values('date').reset_index(drop=True)
    n = len(g)
    if n < 2:
        logging.error(f"{g['ticker'].iloc[0]} - insufficient data (<2 rows)")
        return None

    ticker = g['ticker'].iloc[0]
    results['ticker'] = ticker
    results['date'] = g['date'].iloc[-1]
    results['close_price'] = g['close'].iloc[-1]
    results['volume_today'] = g['volume'].iloc[-1]
    results['volume_prev_day'] = g['volume'].iloc[-2] if n >= 2 else None

    def pct_change(idx_back):
        if n > idx_back:
            return (g['close'].iloc[-1] / g['close'].iloc[-1 - idx_back] - 1) * 100
        return None

    results['daily_change_pct'] = pct_change(1)
    results['daily_change_abs'] = g['close'].iloc[-1] - g['close'].iloc[-2] if n >= 2 else None
    results['weekly_change_pct'] = pct_change(5)
    results['1m_change_pct'] = pct_change(21)
    results['3m_change_pct'] = pct_change(63)
    results['6m_change_pct'] = pct_change(126)

    # SMA / EMA
    g['sma_7'] = g['close'].rolling(window=7, min_periods=1).mean()
    g['sma_21'] = g['close'].rolling(window=21, min_periods=1).mean()
    g['sma_63'] = g['close'].rolling(window=63, min_periods=1).mean()
    g['sma_65'] = g['close'].rolling(window=65, min_periods=1).mean()
    g['sma_126'] = g['close'].rolling(window=126, min_periods=1).mean()

    g['ema_10'] = g['close'].ewm(span=10, adjust=False).mean()
    g['ema_20'] = g['close'].ewm(span=20, adjust=False).mean()
    g['ema_50'] = g['close'].ewm(span=50, adjust=False).mean()
    g['ema_200'] = g['close'].ewm(span=200, adjust=False).mean()

    results['sma_7'] = g['sma_7'].iloc[-1]
    results['sma_21'] = g['sma_21'].iloc[-1]
    results['sma_63'] = g['sma_63'].iloc[-1]
    results['sma_65'] = g['sma_65'].iloc[-1]
    results['sma_126'] = g['sma_126'].iloc[-1]

    results['ema_10'] = g['ema_10'].iloc[-1]
    results['ema_20'] = g['ema_20'].iloc[-1]
    results['ema_50'] = g['ema_50'].iloc[-1]
    results['ema_200'] = g['ema_200'].iloc[-1]

    # Ratios
    results['sma7_sma65_ratio'] = results['sma_7'] / results['sma_65'] if results['sma_65'] != 0 else None
    results['close_over_sma21'] = results['close_price'] / results['sma_21'] if results['sma_21'] != 0 else None
    results['close_over_sma63'] = results['close_price'] / results['sma_63'] if results['sma_63'] != 0 else None
    results['close_over_sma126'] = results['close_price'] / results['sma_126'] if results['sma_126'] != 0 else None

    # 21-day lookback
    results['close_price_21_days_ago'] = g['close'].iloc[-21] if n >= 21 else None
    results['sma63_21_days_ago'] = g['sma_63'].iloc[-21] if n >= 21 else None
    results['close21_over_sma63_21'] = g['close'].iloc[-21] / g['sma_63'].iloc[-21] if n >= 21 and g['sma_63'].iloc[-21] != 0 else None

    # Average volume 20
    results['avg_volume_20'] = g['volume'].rolling(window=20, min_periods=1).mean().iloc[-1]

    # 252-day high/low
    high_window = g['high'].iloc[-252:] if n >= 252 else g['high']
    low_window = g['low'].iloc[-252:] if n >= 252 else g['low']
    results['high_252'] = high_window.max()
    results['low_252'] = low_window.min()
    results['current_to_252_high'] = results['close_price'] / results['high_252'] if results['high_252'] != 0 else None
    results['current_to_252_low'] = results['close_price'] / results['low_252'] if results['low_252'] != 0 else None

    # ATR14
    try:
        if n >= 14:
            atr = ta.volatility.AverageTrueRange(high=g['high'], low=g['low'], close=g['close'], window=14)
            results['atr_14'] = atr.average_true_range().iloc[-1]
        else:
            results['atr_14'] = None
            logging.error(f"{ticker} - ATR14 skipped (history < 14)")
    except Exception as e:
        results['atr_14'] = None
        logging.error(f"{ticker} - ATR14 computation failed: {e}")

    # RSI14
    try:
        if n >= 14:
            rsi = ta.momentum.RSIIndicator(close=g['close'], window=14)
            results['rsi_14'] = rsi.rsi().iloc[-1]
        else:
            results['rsi_14'] = None
            logging.error(f"{ticker} - RSI14 skipped (history < 14)")
    except Exception as e:
        results['rsi_14'] = None
        logging.error(f"{ticker} - RSI14 computation failed: {e}")

    # ADX14
    try:
        if n >= 14:
            adx = ta.trend.ADXIndicator(high=g['high'], low=g['low'], close=g['close'], window=14)
            results['adx_14'] = adx.adx().iloc[-1]
        else:
            results['adx_14'] = None
            logging.error(f"{ticker} - ADX14 skipped (history < 14)")
    except Exception as e:
        results['adx_14'] = None
        logging.error(f"{ticker} - ADX14 computation failed: {e}")

    return pd.DataFrame([results])


def compute_indicators(df):
    all_rows = []
    grouped = df.groupby('ticker', sort=False)
    for ticker, g in grouped:
        row = compute_indicators_for_ticker(g)
        if row is not None:
            all_rows.append(row)
    if all_rows:
        res_df = pd.concat(all_rows, ignore_index=True)
        # Relative Strength percentile (6m_change fallback 3m)
        if '6m_change_pct' in res_df.columns and not res_df['6m_change_pct'].isna().all():
            res_df['rs_percentile'] = res_df['6m_change_pct'].rank(pct=True) * 100.0
        else:
            res_df['rs_percentile'] = res_df['3m_change_pct'].rank(pct=True) * 100.0
        return res_df
    else:
        return pd.DataFrame()
