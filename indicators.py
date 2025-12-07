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
    """
    Copy master DB to working DB if master is newer or working doesn't exist.
    Returns True if a copy occurred, False otherwise.
    """
    if not os.path.exists(MASTER_DB):
        msg = f"Master DB not found at {MASTER_DB}"
        logging.error(msg)
        raise FileNotFoundError(msg)

    if (not os.path.exists(WORKING_DB)) or (os.path.getmtime(MASTER_DB) > os.path.getmtime(WORKING_DB)):
        shutil.copyfile(MASTER_DB, WORKING_DB)
        logging.info(f"Copied master DB to working DB: {WORKING_DB}")
        return True

    logging.info("Working DB is up-to-date; no copy performed.")
    return False

# === Read raw prices ===
def read_raw_prices(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Read raw_prices table. Expects columns:
    date, open, high, low, close, volume, ticker
    Returns DataFrame sorted by ticker, date.
    """
    df = pd.read_sql("SELECT date, open, high, low, close, volume, ticker FROM raw_prices", conn, parse_dates=['date'])
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(['ticker', 'date']).reset_index(drop=True)
    return df

# === Indicator computation ===
def compute_indicators_in_memory(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each ticker compute requested metrics and return one-row-per-ticker DataFrame.
    If insufficient lookback exists for a metric, that field will be NaN (no exception).
    """
    out = []
    grouped = df.groupby('ticker', sort=False)

    for ticker, g in grouped:
        g = g.sort_values('date').reset_index(drop=True)
        n = len(g)
        if n < 1:
            continue

        # Ensure numeric types
        for col in ['open', 'high', 'low', 'close', 'volume']:
            g[col] = pd.to_numeric(g[col], errors='coerce')

        # EMAs
        g['ema10'] = g['close'].ewm(span=10, adjust=False).mean()
        g['ema20'] = g['close'].ewm(span=20, adjust=False).mean()
        g['ema50'] = g['close'].ewm(span=50, adjust=False).mean()
        g['ema200'] = g['close'].ewm(span=200, adjust=False).mean()

        # SMAs with lookback windows
        g['sma7'] = g['close'].rolling(window=7, min_periods=1).mean()
        g['sma21'] = g['close'].rolling(window=21, min_periods=1).mean()
        g['sma63'] = g['close'].rolling(window=63, min_periods=1).mean()
        g['sma126'] = g['close'].rolling(window=126, min_periods=1).mean()
        g['sma63_alt'] = g['close'].rolling(window=63, min_periods=1).mean()  # alias if needed

        # ATR (14) - True Range uses previous close
        g['prev_close'] = g['close'].shift(1)
        tr1 = (g['high'] - g['low']).abs()
        tr2 = (g['high'] - g['prev_close']).abs()
        tr3 = (g['low'] - g['prev_close']).abs()
        g['tr'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        g['atr14'] = g['tr'].rolling(window=14, min_periods=1).mean()

        # Volume metrics
        g['volume_avg20'] = g['volume'].rolling(window=20, min_periods=1).mean()
        # volume spike boolean per row
        g['volume_spike_flag'] = g['volume'] > 1.5 * g['volume_avg20']
        g['volume_prev'] = g['volume'].shift(1)
        g['today_vol_gt_yesterday'] = g['volume'] > g['volume_prev']

        # Last row and previous row (if exists)
        last = g.iloc[-1]
        prev = g.iloc[-2] if n >= 2 else None

        # Helper to compute pct change from offset (number of trading days back). Returns NaN if not enough rows.
        def pct_change_from_offset(offset_days: int) -> Optional[float]:
            idx = n - 1 - offset_days
            if idx >= 0:
                prev_close = float(g.iloc[idx]['close'])
                if prev_close == 0:
                    return np.nan
                return (float(last['close']) / prev_close - 1.0) * 100.0
            return np.nan

        # Helper safe read of column at offset
        def value_at_offset(col: str, offset_days: int) -> Optional[float]:
            idx = n - 1 - offset_days
            if idx >= 0:
                return float(g.iloc[idx][col])
            return np.nan

        # 252-day high/low only if at least some data up to 252 exists, else NaN (user requested conditional)
        if n >= 252:
            last_252 = g.tail(252)
            high_252 = float(last_252['high'].max())
            low_252 = float(last_252['low'].min())
        else:
            # If not enough history, leave as NaN
            high_252 = np.nan
            low_252 = np.nan

        # Build row
        row = {
            'ticker': ticker,
            'date': last['date'],
            'close': float(last['close']) if not pd.isna(last['close']) else np.nan,
            'volume': float(last['volume']) if not pd.isna(last['volume']) else np.nan,
            'previous_volume': float(prev['volume']) if (prev is not None and not pd.isna(prev['volume'])) else np.nan,
            'daily_change_pct': pct_change_from_offset(1),
            'daily_change_rupees': (float(last['close']) - float(prev['close'])) if (prev is not None and not pd.isna(prev['close'])) else np.nan,
            'atr': float(last['atr14']) if not pd.isna(last['atr14']) else np.nan,
            'weekly_change_pct': pct_change_from_offset(5),
            '1m_change_pct': pct_change_from_offset(22),
            '3m_change_pct': pct_change_from_offset(66),
            '6m_change_pct': pct_change_from_offset(132),
            'sma7_sma63_ratio': (float(last['sma7']) / float(last['sma63'])) if (not pd.isna(last['sma7']) and not pd.isna(last['sma63']) and last['sma63'] != 0) else np.nan,
            '252_days_high': high_252,
            '252_days_low': low_252,
            'ema10': float(last['ema10']) if not pd.isna(last['ema10']) else np.nan,
            'ema20': float(last['ema20']) if not pd.isna(last['ema20']) else np.nan,
            'ema50': float(last['ema50']) if not pd.isna(last['ema50']) else np.nan,
            'ema200': float(last['ema200']) if not pd.isna(last['ema200']) else np.nan,
            'volume_avg20': float(last['volume_avg20']) if not pd.isna(last['volume_avg20']) else np.nan,
            'volume_spike_1p5x_avg20': bool(last['volume_spike_flag']) if not pd.isna(last['volume_spike_flag']) else False,
            'today_volume_gt_yesterday': bool(last['today_vol_gt_yesterday']) if not pd.isna(last['today_vol_gt_yesterday']) else False,
            'close_div_sma21': (float(last['close']) / float(last['sma21'])) if (not pd.isna(last['sma21']) and last['sma21'] != 0) else np.nan,
            'close_div_sma63': (float(last['close']) / float(last['sma63'])) if (not pd.isna(last['sma63']) and last['sma63'] != 0) else np.nan,
            'close_div_sma126': (float(last['close']) / float(last['sma126'])) if (not pd.isna(last['sma126']) and last['sma126'] != 0) else np.nan,
            # close 21 days ago / sma126 21 days ago: only if index exists
            'close21_days_ago_div_sma126_21_days_ago': ( (float(value_at_offset('close', 21)) / float(g.iloc[n-1-21]['sma126'])) if (n-1-21 >= 0 and not pd.isna(g.iloc[n-1-21]['sma126']) and g.iloc[n-1-21]['sma126'] != 0) else np.nan ),
        }

        out.append(row)

    # Create DataFrame
    res = pd.DataFrame(out)

    # Relative strength (percentile) — choose the longest lookback available
    if not res.empty:
        if '6m_change_pct' in res.columns and res['6m_change_pct'].notna().any():
            res['relative_strength'] = res['6m_change_pct'].rank(pct=True) * 100.0
        elif '3m_change_pct' in res.columns and res['3m_change_pct'].notna().any():
            res['relative_strength'] = res['3m_change_pct'].rank(pct=True) * 100.0
        elif '1m_change_pct' in res.columns and res['1m_change_pct'].notna().any():
            res['relative_strength'] = res['1m_change_pct'].rank(pct=True) * 100.0
        else:
            res['relative_strength'] = np.nan
    else:
        res['relative_strength'] = pd.Series(dtype=float)

    # Re-order columns for readability (some preferred order)
    preferred_order = [
        'ticker', 'date', 'close', 'volume', 'previous_volume',
        'daily_change_pct', 'daily_change_rupees', 'atr',
        'weekly_change_pct', '1m_change_pct', '3m_change_pct', '6m_change_pct',
        'ema10', 'ema20', 'ema50', 'ema200',
        'sma7_sma63_ratio',
        'close_div_sma21', 'close_div_sma63', 'close_div_sma126',
        'close21_days_ago_div_sma126_21_days_ago',
        '252_days_high', '252_days_low',
        'volume_avg20', 'volume_spike_1p5x_avg20', 'today_volume_gt_yesterday',
        'relative_strength'
    ]
    # Keep the ordered columns that exist and then all others
    cols_existing = [c for c in preferred_order if c in res.columns]
    remaining = [c for c in res.columns if c not in cols_existing]
    res = res[cols_existing + remaining]

    return res

# === Write out computed table ===
def write_computed_table(res_df: pd.DataFrame):
    """Write computed DataFrame to WORKING_DB as table 'computed' (replace)."""
    conn = sqlite3.connect(WORKING_DB)
    try:
        res_df.to_sql("computed", conn, if_exists="replace", index=False)
        logging.info(f"Wrote {len(res_df)} rows to computed table in {WORKING_DB}")
    finally:
        conn.close()

# === Orchestration ===
def ensure_computed_table():
    """
    Sync master->working (if needed) and compute indicators into computed table.
    Call this on-demand (app button) or via cron.
    """
    sync_master_to_working()
    conn = sqlite3.connect(WORKING_DB)
    try:
        df = read_raw_prices(conn)
        computed = compute_indicators_in_memory(df)
        write_computed_table(computed)
    finally:
        conn.close()
