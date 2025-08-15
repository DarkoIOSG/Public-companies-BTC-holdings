import streamlit as st
import pandas as pd

# Page setup
st.set_page_config(page_title="Public Companies BTC Holdings Dashboard", layout="wide")

# Load data
@st.cache_data
def load_data():
    df = pd.read_csv("bitbo_historical.csv", parse_dates=["Date"])
    return df

df = load_data()

st.title("📊 Public Companies Bitcoin Holdings")
st.caption("Data scraped from Bitbo — showing daily public company BTC holdings.")

# Sidebar filters
st.sidebar.header("Filters")

# Select a date to view
dates = sorted(df["Date"].unique(), reverse=True)
selected_date = st.sidebar.selectbox("Select Date", dates)

# Filter for that date
df_selected = df[df["Date"] == selected_date]

# Show totals
total_btc = df_selected["# of BTC"].sum()
total_value = df_selected["Value Today"].sum()

col1, col2 = st.columns(2)
col1.metric("Total BTC", f"{total_btc:,.0f}")
col2.metric("Total Value", f"${total_value:,.2f}")

# Search box
search_term = st.sidebar.text_input("Search Company")
if search_term:
    df_selected = df_selected[df_selected["Entity"].str.contains(search_term, case=False, na=False)]

# Show table
st.dataframe(df_selected, use_container_width=True)

# Historical total BTC chart
st.subheader("📈 Total BTC Over Time")
historical_totals = df.groupby("Date")["# of BTC"].sum().reset_index()
st.line_chart(historical_totals.set_index("Date"))
