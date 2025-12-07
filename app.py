import sqlite3
import pandas as pd
import streamlit as st
from indicators import ensure_computed_table, WORKING_DB, sync_master_to_working
import traceback

st.set_page_config(page_title="📊 NSE Screener", layout="wide")
st.title("📊 NSE Screener")


# ---------- COMPUTE BUTTON ----------
if st.button("⚙️ Calculate / Refresh Computed Table"):
    with st.spinner("Computing indicators..."):
        try:
            sync_master_to_working()
            ensure_computed_table()
            load_data.clear()  # ?? Clear cache so next load fetches fresh DB
            st.success("✔ Indicator computation completed.")
        except Exception as e:
            st.error("❌ Error during computation.")
            st.text(traceback.format_exc())


# ---------- LOAD DB ----------
@st.cache_data(ttl=30)
def load_data():
    conn = sqlite3.connect(WORKING_DB)
    df = pd.read_sql("SELECT * FROM computed", conn, parse_dates=['date'])
    conn.close()
    return df


try:
    df = load_data()
except Exception as e:
    st.error(f"⚠ Could not load computed data: {e}")
    st.stop()

if df.empty:
    st.warning("⚠ No computed data available. Please run computation.")
    st.stop()


# ---------- SIDEBAR FILTERS ----------
st.sidebar.title("🔍 Filters")

# Search
search = st.sidebar.text_input("Search Ticker (contains):", "")

# Price & Volume
st.sidebar.subheader("💰 Price / Volume")
min_price = st.sidebar.number_input("Min Close Price", value=float(df.close.min()))
max_price = st.sidebar.number_input("Max Close Price", value=float(df.close.max()))
min_volume = st.sidebar.number_input("Min Volume", value=0)

# Trend Filters
st.sidebar.subheader("📈 Trend Filters (EMA)")
chk_ema10 = st.sidebar.checkbox("Close > EMA10")
chk_ema20 = st.sidebar.checkbox("Close > EMA20")
chk_ema50 = st.sidebar.checkbox("Close > EMA50")
chk_ema200 = st.sidebar.checkbox("Close > EMA200")

# ADVANCED
with st.sidebar.expander("⚙️ Advanced Filters"):

    # % Change
    min_daily = st.number_input("Min Daily % Change", value=float(df.daily_change_pct.min()))
    max_daily = st.number_input("Max Daily % Change", value=float(df.daily_change_pct.max()))
    abs_move_min = st.number_input("Min Absolute Move (₹)", value=0.0)

    # Weekly / Monthly
    min_week = st.number_input("Min Weekly % Change", value=float(df.weekly_change_pct.min()))
    max_week = st.number_input("Max Weekly % Change", value=float(df.weekly_change_pct.max()))
    min_month = st.number_input("Min 1M % Change", value=float(df["1m_change_pct"].min()))
    max_month = st.number_input("Max 1M % Change", value=float(df["1m_change_pct"].max()))

    # ATR
    atr_min = st.number_input("Min ATR", value=float(df.atr.min()))
    atr_max = st.number_input("Max ATR", value=float(df.atr.max()))

    # Relative Strength (Always Required)
    rs_min = st.number_input("Min Relative Strength", value=70.0)
    rs_max = st.number_input("Max Relative Strength", value=100.0)

# ---------- RATIO FILTER TOGGLES ----------
st.sidebar.subheader("📏 Ratio Filters")

ratio_filters = {
    "sma7_sma63_ratio": st.sidebar.checkbox("Enable SMA7 / SMA63 Ratio"),
    "close_div_sma21": st.sidebar.checkbox("Enable Close / SMA21"),
    "close_div_sma63": st.sidebar.checkbox("Enable Close / SMA63"),
    "close_div_sma126": st.sidebar.checkbox("Enable Close / SMA126"),
    "close21_days_ago_div_sma126_21_days_ago": st.sidebar.checkbox("Enable (Close21/SMA126-21) Ratio"),
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

# Search filter
if search:
    filtered = filtered[filtered["ticker"].str.contains(search, case=False, na=False)]

# Standard numeric filters
filtered = filtered[
    (filtered.close.between(min_price, max_price)) &
    (filtered.volume >= min_volume) &
    (filtered.daily_change_pct.between(min_daily, max_daily)) &
    (filtered.weekly_change_pct.between(min_week, max_week)) &
    (filtered["1m_change_pct"].between(min_month, max_month)) &
    (filtered.atr.between(atr_min, atr_max)) &
    (filtered.relative_strength.between(rs_min, rs_max))
]

# Absolute move filter
filtered = filtered[filtered.daily_change_rupees.abs() >= abs_move_min]

# EMA Trend Rules
if chk_ema10:
    filtered = filtered[filtered.close > filtered.ema10]
if chk_ema20:
    filtered = filtered[filtered.close > filtered.ema20]
if chk_ema50:
    filtered = filtered[filtered.close > filtered.ema50]
if chk_ema200:
    filtered = filtered[filtered.close > filtered.ema200]

# Apply ratio toggles
for key, (low, high) in ratio_ranges.items():
    filtered = filtered[filtered[key].between(low, high)]


# ---------- HIDE INTERNAL COLUMNS ----------
hidden_columns = [
    "volume_spike_1p5x_avg20",
    "today_volume_gt_yesterday",]

filtered_visible = filtered.drop(columns=[c for c in hidden_columns if c in filtered.columns])


# ---------- SORT ----------
st.subheader("📌 Sorting")
cols = filtered_visible.columns.tolist()
sort_col = st.selectbox("Sort Column", cols, index=cols.index("relative_strength"))
order = st.radio("Order", ["Descending", "Ascending"])

filtered_visible = filtered_visible.sort_values(by=sort_col, ascending=(order == "Ascending"))


# ---------- DISPLAY ----------
st.markdown(f"### ✔ Results: {len(filtered_visible)} stocks found")
st.dataframe(filtered_visible, use_container_width=True)


# ---------- DOWNLOAD ----------
# Add extra column without .NS
download_df = filtered_visible.copy()
download_df["ticker_no_ns"] = download_df["ticker"].str.replace(".NS", "", regex=False)

csv = filtered_visible.to_csv(index=False)
st.download_button("⬇ Download Filtered CSV", csv, "filtered_stocks.csv", "text/csv")
