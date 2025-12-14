import sqlite3
import pandas as pd
import streamlit as st
from indicators import ensure_computed_table, WORKING_DB, sync_master_to_working
import traceback

st.set_page_config(page_title="ðŸ“Š NSE Screener", layout="wide")
st.title("ðŸ“Š NSE Screener")

# ---------- NOTE / HELP ----------
with st.expander("Notes / Instructions", expanded=False):
    st.markdown("""
    - TI63 > 1.05 FOR BULLISH, < 0.95 FOR BEARISH  
    - Daily change Â±1%  
    - MDT21 AND MDT50 >= 1.18  
    """)

# ---------- COMPUTE BUTTON ----------
if st.button("âš™ï¸ Calculate / Refresh Computed Table"):
    with st.spinner("Computing indicators..."):
        try:
            sync_master_to_working()
            ensure_computed_table()
            load_data.clear()
            st.success("âœ” Indicator computation completed.")
        except Exception:
            st.error("âŒ Error during computation.")
            st.text(traceback.format_exc())

# ---------- LOAD DB ----------
@st.cache_data(ttl=30)
def load_data():
    conn = sqlite3.connect(WORKING_DB)
    df = pd.read_sql("SELECT * FROM computed", conn, parse_dates=["date"])
    conn.close()
    return df

try:
    df = load_data()
except Exception as e:
    st.error(f"âš  Could not load computed data: {e}")
    st.stop()

if df.empty:
    st.warning("âš  No computed data available. Please run computation.")
    st.stop()

# ---------- SIDEBAR FILTERS ----------
st.sidebar.title("ðŸ” Filters")

# --- Search ---
search = st.sidebar.text_input("Search Ticker (contains):", "")

# --- Price & Volume ---
st.sidebar.subheader("ðŸ’° Price & Volume")
min_price = st.sidebar.number_input("Min Close Price", value=float(df.close.min()))
max_price = st.sidebar.number_input("Max Close Price", value=float(df.close.max()))
min_volume = st.sidebar.number_input("Min Volume", value=0)

# --- Turnover Filter (NEW) ---
st.sidebar.subheader("ðŸ’° Turnover Filter")
min_turnover = st.sidebar.number_input(
    "Min Turnover (Close Ã— Volume)",
    value=0.0
)

# --- Volume Filters ---
st.sidebar.subheader("ðŸ“Š Volume Filters")
chk_vol_yesterday = st.sidebar.checkbox("Volume > Previous Day")
chk_vol_spike = st.sidebar.checkbox("Volume Spike (>1.5 Ã— Avg20)")

# --- EMA Trend Filters ---
st.sidebar.subheader("ðŸ“ˆ EMA Trend Filters")
chk_ema10 = st.sidebar.checkbox("Close > EMA10")
chk_ema20 = st.sidebar.checkbox("Close > EMA20")
chk_ema50 = st.sidebar.checkbox("Close > EMA50")
chk_ema200 = st.sidebar.checkbox("Close > EMA200")

# --- % Change Filters ---
st.sidebar.subheader("ðŸ“‰ % Change Filters")
min_daily = st.sidebar.number_input("Min Daily % Change", value=float(df.daily_change_pct.min()))
max_daily = st.sidebar.number_input("Max Daily % Change", value=float(df.daily_change_pct.max()))
abs_move_min = st.sidebar.number_input("Min Absolute Move (â‚¹)", value=0.0)
min_week = st.sidebar.number_input("Min Weekly % Change", value=float(df.weekly_change_pct.min()))
max_week = st.sidebar.number_input("Max Weekly % Change", value=float(df.weekly_change_pct.max()))
min_1m = st.sidebar.number_input("Min 1M % Change", value=float(df["1m_change_pct"].min()))
max_1m = st.sidebar.number_input("Max 1M % Change", value=float(df["1m_change_pct"].max()))
min_3m = st.sidebar.number_input("Min 3M % Change", value=float(df["3m_change_pct"].min()))
max_3m = st.sidebar.number_input("Max 3M % Change", value=float(df["3m_change_pct"].max()))
min_6m = st.sidebar.number_input("Min 6M % Change", value=float(df["6m_change_pct"].min()))
max_6m = st.sidebar.number_input("Max 6M % Change", value=float(df["6m_change_pct"].max()))

# --- MDT Filters ---
st.sidebar.subheader("ðŸ“Š MDT Filters")
min_mdt21 = st.sidebar.number_input("Min MDT21", value=float(df["MDT21"].min()))
max_mdt21 = st.sidebar.number_input("Max MDT21", value=float(df["MDT21"].max()))
min_mdt50 = st.sidebar.number_input("Min MDT50", value=float(df["MDT50"].min()))
max_mdt50 = st.sidebar.number_input("Max MDT50", value=float(df["MDT50"].max()))

# --- 252 Day Range Filters (NEW) ---
st.sidebar.subheader("ðŸ“Š 252-Day Range Filters")
min_pct_above_low = st.sidebar.number_input(
    "% Above 252D Low (Min)",
    value=0.0
)
max_pct_below_high = st.sidebar.number_input(
    "% Below 252D High (Max)",
    value=100.0
)

# --- New High Filters (NEW) ---
st.sidebar.subheader("ðŸš€ New Highs")
chk_1m_high = st.sidebar.checkbox("New 1M High")
chk_3m_high = st.sidebar.checkbox("New 3M High")
chk_6m_high = st.sidebar.checkbox("New 6M High")

# --- ATR & Relative Strength ---
st.sidebar.subheader("ðŸ“ ATR & Relative Strength")
atr_min = st.sidebar.number_input("Min ATR", value=float(df.atr.min()))
atr_max = st.sidebar.number_input("Max ATR", value=float(df.atr.max()))
rs_min = st.sidebar.number_input("Min Relative Strength", value=70.0)
rs_max = st.sidebar.number_input("Max Relative Strength", value=100.0)

# --- Optional Ratio Filters ---
st.sidebar.subheader("ðŸ“ Optional Ratio Filters")
ratio_filters = {
    "TI63": st.sidebar.checkbox("Enable TI63"),
    "1M_momentum": st.sidebar.checkbox("Enable 1M Momentum"),
    "3M_momentum": st.sidebar.checkbox("Enable 3M Momentum"),
    "6M_momentum": st.sidebar.checkbox("Enable 6M Momentum"),
    "MDT21": st.sidebar.checkbox("Enable MDT21"),
    "MDT50": st.sidebar.checkbox("Enable MDT50"),
}

ratio_ranges = {}
for key, active in ratio_filters.items():
    if active:
        st.sidebar.markdown(f"**{key} RANGE:**")
        min_v = st.sidebar.number_input(f"Min ({key})", value=float(df[key].min()))
        max_v = st.sidebar.number_input(f"Max ({key})", value=float(df[key].max()))
        ratio_ranges[key] = (min_v, max_v)

# ---------- APPLY FILTERS ----------
filtered = df.copy()

# Search
if search:
    filtered = filtered[filtered["ticker"].str.contains(search, case=False, na=False)]

# Core numeric filters (UNCHANGED, only appended)
filtered = filtered[
    (filtered.close.between(min_price, max_price)) &
    (filtered.volume >= min_volume) &
    ((filtered.close * filtered.volume) >= min_turnover) &
    (filtered.daily_change_pct.between(min_daily, max_daily)) &
    (filtered.weekly_change_pct.between(min_week, max_week)) &
    (filtered["1m_change_pct"].between(min_1m, max_1m)) &
    (filtered["3m_change_pct"].between(min_3m, max_3m)) &
    (filtered["6m_change_pct"].between(min_6m, max_6m)) &
    (filtered.atr.between(atr_min, atr_max)) &
    (filtered.relative_strength.between(rs_min, rs_max)) &
    (filtered.daily_change_rupees.abs() >= abs_move_min) &
    (filtered.MDT21.between(min_mdt21, max_mdt21)) &
    (filtered.MDT50.between(min_mdt50, max_mdt50))
]

# --- 252 Day % Filters ---
filtered = filtered[
    ((filtered.close - filtered["252_days_low"]) / filtered["252_days_low"] * 100) >= min_pct_above_low
]

filtered = filtered[
    ((filtered["252_days_high"] - filtered.close) / filtered["252_days_high"] * 100) <= max_pct_below_high
]

# EMA Trend Rules
if chk_ema10:
    filtered = filtered[filtered.close > filtered.ema10]
if chk_ema20:
    filtered = filtered[filtered.close > filtered.ema20]
if chk_ema50:
    filtered = filtered[filtered.close > filtered.ema50]
if chk_ema200:
    filtered = filtered[filtered.close > filtered.ema200]

# Volume Filters
if chk_vol_yesterday:
    filtered = filtered[filtered.volume > filtered.previous_volume]
if chk_vol_spike:
    filtered = filtered[filtered.volume > 1.5 * filtered.volume_avg20]

# New High Filters
if chk_1m_high:
    filtered = filtered[filtered.close >= filtered["252_days_high"] * 0.99]
if chk_3m_high:
    filtered = filtered[filtered.close >= filtered["252_days_high"] * 0.97]
if chk_6m_high:
    filtered = filtered[filtered.close >= filtered["252_days_high"] * 0.95]

# Optional ratio toggles
for key, (low, high) in ratio_ranges.items():
    filtered = filtered[filtered[key].between(low, high)]

# ---------- HIDE INTERNAL COLUMNS ----------
hidden_columns = ["volume_avg20", "previous_volume"]
filtered_visible = filtered.drop(columns=[c for c in hidden_columns if c in filtered.columns])

# ---------- SORT ----------
st.subheader("ðŸ“Œ Sorting")
cols = filtered_visible.columns.tolist()
sort_col = st.selectbox("Sort Column", cols, index=cols.index("relative_strength"))
order = st.radio("Order", ["Descending", "Ascending"])
filtered_visible = filtered_visible.sort_values(by=sort_col, ascending=(order == "Ascending"))

# ---------- DISPLAY ----------
st.markdown(f"### âœ” Results: {len(filtered_visible)} stocks found")
st.dataframe(filtered_visible, use_container_width=True)

# ---------- DOWNLOAD ----------
csv = filtered_visible.to_csv(index=False)
st.download_button("â¬‡ Download Filtered CSV", csv, "filtered_stocks.csv", "text/csv")
