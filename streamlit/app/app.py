import streamlit as st
import pandas as pd
import time
from streamlit_autorefresh import st_autorefresh
from data_processing import fetch_candlestick_data, fetch_live_price_data, fetch_volume_data
from chart_utils import create_candlestick_chart, create_volume_chart, create_live_price_chart
from config import STOCK_TICKERS, POWER_BI_EMBED_URL, CANDLESTICK_UPDATE_INTERVAL, LIVE_PRICE_UPDATE_INTERVAL

# Streamlit App Configuration
st.set_page_config(layout="wide", page_title="Real-Time Stock Dashboard")

# Auto-refresh every X seconds
st_autorefresh(interval=LIVE_PRICE_UPDATE_INTERVAL * 1000, limit=100, key="live_price_refresh")

# Sidebar - Stock Selection
st.sidebar.title("Stock Selection")
selected_ticker = st.sidebar.selectbox("Select a Stock", STOCK_TICKERS)

# Containers for dynamic updates
candlestick_container = st.empty()
volume_container = st.empty()
live_price_container = st.empty()

# Power BI Dashboard
st.markdown("---")
st.subheader("Power BI Dashboard")
st.components.v1.html(
    f'<iframe title="Power BI" width="100%" height="800" src="{POWER_BI_EMBED_URL}" frameborder="0" allowFullScreen="true"></iframe>',
    height=800,
)

# Session state to persist previous data across refreshes
if "candlestick_df" not in st.session_state:
    st.session_state["candlestick_df"] = fetch_candlestick_data(selected_ticker)
if "volume_df" not in st.session_state:
    st.session_state["volume_df"] = fetch_volume_data(selected_ticker)
if "live_price_df" not in st.session_state:
    st.session_state["live_price_df"] = fetch_live_price_data(selected_ticker)
if "last_candle_update" not in st.session_state:
    st.session_state["last_candle_update"] = time.time()

# Update live price
new_live_price_df = fetch_live_price_data(selected_ticker)
if not new_live_price_df.empty:
    st.session_state["live_price_df"] = pd.concat([
        new_live_price_df.iloc[-1:], st.session_state["live_price_df"]
    ]).iloc[:50]  # Keep only latest 50 rows

# Update candlestick and volume every 5 minutes
if time.time() - st.session_state["last_candle_update"] > CANDLESTICK_UPDATE_INTERVAL:
    new_candlestick_df = fetch_candlestick_data(selected_ticker)
    new_volume_df = fetch_volume_data(selected_ticker)

    if not new_candlestick_df.empty:
        st.session_state["candlestick_df"] = pd.concat([
            new_candlestick_df.iloc[-1:], st.session_state["candlestick_df"]
        ]).iloc[:50]

    if not new_volume_df.empty:
        st.session_state["volume_df"] = pd.concat([
            new_volume_df.iloc[-1:], st.session_state["volume_df"]
        ]).iloc[:50]

    st.session_state["last_candle_update"] = time.time()

# Display candlestick and volume charts
with candlestick_container.container():
    col1, col2 = st.columns(2)
    with col1:
        st.subheader(f"Candlestick Chart - {selected_ticker}")
        st.plotly_chart(create_candlestick_chart(st.session_state["candlestick_df"]), use_container_width=True)
    with col2:
        st.subheader(f"Volume Chart - {selected_ticker}")
        st.plotly_chart(create_volume_chart(st.session_state["volume_df"]), use_container_width=True)

# Display live price chart
with live_price_container.container():
    st.subheader(f"Live Price - {selected_ticker}")
    st.plotly_chart(create_live_price_chart(st.session_state["live_price_df"]), use_container_width=True)






# import streamlit as st
# import pandas as pd
# import time
# from streamlit_autorefresh import st_autorefresh
# from data_processing import fetch_candlestick_data, fetch_live_price_data, fetch_volume_data
# from chart_utils import create_candlestick_chart, create_volume_chart, create_live_price_chart
# from config import STOCK_TICKERS, POWER_BI_EMBED_URL, CANDLESTICK_UPDATE_INTERVAL, LIVE_PRICE_UPDATE_INTERVAL
# # Streamlit App Configuration
# st.set_page_config(layout="wide", page_title="Real-Time Stock Dashboard")
# st_autorefresh(interval=LIVE_PRICE_UPDATE_INTERVAL * 1000, limit=100, key="live_price_refresh")

# # Sidebar - Stock Selection
# st.sidebar.title("Stock Selection")
# selected_ticker = st.sidebar.selectbox("Select a Stock", STOCK_TICKERS)

# # Containers for dynamic updates
# candlestick_container = st.empty()
# volume_container = st.empty()
# live_price_container = st.empty()

# # Power BI Dashboard
# st.markdown("---")
# st.subheader("Power BI Dashboard")
# st.components.v1.html(
#     f'<iframe title="Power BI" width="100%" height="800" src="{POWER_BI_EMBED_URL}" frameborder="0" allowFullScreen="true"></iframe>',
#     height=800,
# )


# # Initialize data
# candlestick_df = fetch_candlestick_data(selected_ticker)
# volume_df = fetch_volume_data(selected_ticker)
# live_price_df = fetch_live_price_data(selected_ticker)

# # Display initial charts
# with candlestick_container.container():
#     col1, col2 = st.columns(2)
#     with col1:
#         st.subheader(f"Candlestick Chart - {selected_ticker}")
#         candlestick_chart = st.plotly_chart(create_candlestick_chart(candlestick_df), use_container_width=True)
#     with col2:
#         st.subheader(f"Volume Chart - {selected_ticker}")
#         volume_chart = st.plotly_chart(create_volume_chart(volume_df), use_container_width=True)

# with live_price_container.container():
#     st.subheader(f"Live Price - {selected_ticker}")
#     live_price_chart = st.plotly_chart(create_live_price_chart(live_price_df), use_container_width=True)

# # Auto-refresh loop
# while True:
#     time.sleep(LIVE_PRICE_UPDATE_INTERVAL)  # Update live price every 10 seconds
    
#     # Fetch latest live price
#     new_live_price_df = fetch_live_price_data(selected_ticker)
    
#     # Append new row & remove oldest row to maintain size
#     if not new_live_price_df.empty:
#         live_price_df = pd.concat([new_live_price_df.iloc[-1:], live_price_df]).iloc[:-1]
    
#     # Update live price chart
#     with live_price_container.container():
#         st.plotly_chart(create_live_price_chart(live_price_df), use_container_width=True)

#     # Update candlestick & volume charts every 5 minutes
#     if int(time.time()) % CANDLESTICK_UPDATE_INTERVAL < LIVE_PRICE_UPDATE_INTERVAL:
#         new_candlestick_df = fetch_candlestick_data(selected_ticker)
#         new_volume_df = fetch_volume_data(selected_ticker)
        
#         if not new_candlestick_df.empty:
#             candlestick_df = pd.concat([new_candlestick_df.iloc[-1:], candlestick_df]).iloc[:-1]
#             volume_df = pd.concat([new_volume_df.iloc[-1:], volume_df]).iloc[:-1]
        
#         with candlestick_container.container():
#             col1, col2 = st.columns(2)  
#             with col1:
#                 st.subheader(f"Candlestick Chart - {selected_ticker}")
#                 st.plotly_chart(create_candlestick_chart(candlestick_df), use_container_width=True)
#             with col2:
#                 st.subheader(f"Volume Chart - {selected_ticker}")
#                 st.plotly_chart(create_volume_chart(volume_df), use_container_width=True)
