import streamlit as st
import pandas as pd
import plotly.express as px
import os
import glob

# Set Streamlit page config
st.set_page_config(page_title="Crypto Pipeline Dashboard", layout="wide", page_icon="📈")

st.title("Crypto Market Data Dashboard")
st.markdown("A lightweight visualization layer built on top of our Data Lake Parquet files.")

# --- LOAD DATA ---
@st.cache_data(ttl=60)
def load_rankings():
    path = os.path.join("data", "processed", "rankings", "top_winners.parquet")
    if os.path.exists(path):
         return pd.read_parquet(path)
    return None

@st.cache_data(ttl=60)
def load_all_sma():
    sma_files = glob.glob(os.path.join("data", "processed", "sma", "**", "*.parquet"), recursive=True)
    if not sma_files:
        return None
    dfs = [pd.read_parquet(f) for f in sma_files]
    df = pd.concat(dfs, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values(by="date")

df_rankings = load_rankings()
df_sma = load_all_sma()

# --- TOP WINNERS SECTION ---
st.subheader("🏆 Top 5 Winners (Last 24h)")
if df_rankings is not None:
    display_df = df_rankings[["winner_rank", "name", "symbol", "priceUsd", "changePercent24Hr"]].copy()
    display_df.columns = ["Rank", "Coin", "Symbol", "Price (USD)", "24h Change (%)"]
    

    styled_df = display_df.style.format({
        "Price (USD)": "${:,.4f}",
        "24h Change (%)": "{:+.2f}%"
    }).background_gradient(subset=["24h Change (%)"], cmap="Greens")
    
    st.dataframe(styled_df, use_container_width=True, hide_index=True)
else:
    st.warning("Ranking data not found. Please run `python transform.py` first.")

st.divider()

# --- SMA CHART SECTION ---
st.subheader(" Price vs 7-Day SMA Trajectory")

if df_sma is not None:
    available_coins = df_sma["coin_id"].unique()
    
    # Create Columns for Top-Level Filter
    col1, col2 = st.columns([1, 4])
    with col1:
        selected_coin = st.selectbox("Select a coin to analyze:", available_coins)
    
    # Filter data
    df_filtered = df_sma[df_sma["coin_id"] == selected_coin]
    
    if not df_filtered.empty:

        fig = px.line(
            df_filtered, 
            x="date", 
            y=["priceUsd", "sma_7d"],
            labels={"value": "Price (USD)", "date": "Date", "variable": "Metric"},
            title=f"{selected_coin.title()} - Daily Price vs 7-Day Moving Average"
        )
        
        # Make the actual price a lighter scatter trace and the SMA a bold line
        fig.data[0].name = "Daily Close Price"
        fig.data[0].line.width = 1
        fig.data[0].line.dash = "dot"
        
        fig.data[1].name = "7-Day SMA"
        fig.data[1].line.width = 3
        
        fig.update_layout(hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)
        
        # Show raw data option
        with st.expander("Show raw historical Parquet data"):
            st.dataframe(df_filtered.sort_values(by="date", ascending=False), use_container_width=True)
    else:
        st.info(f"No SMA data found for {selected_coin}.")
else:
    st.warning("SMA data not found. Please run `python transform.py` first.")

st.caption("Auto-generates charts by directly reading Phase 2 output partitions via PyArrow.")
