import streamlit as st
import pandas as pd

# ===== 1. Page Setup =====
st.set_page_config(
    page_title="Public Companies BTC Holdings Dashboard",
    layout="wide"
)

st.title("📊 Public Companies Bitcoin Holdings")
st.caption("Data scraped from Bitbo — showing daily public company BTC holdings.")

# ===== 2. Load Data =====
@st.cache_data
def load_data():
    df = pd.read_csv("bitbo_historical.csv", parse_dates=["Scrape Date"])
    
    # Convert numeric columns
    df["# of BTC"] = df["# of BTC"].astype(float)
    df["% of 21m"] = df["% of 21m"].replace('%', '', regex=True).astype(float)
    
    # Filter out rows where Entity is None or NaN
    df = df[df["Entity"].notna()]
    
    # Calculate daily change per company
    df = df.sort_values(["Entity", "Scrape Date"])
    df["BTC Change"] = df.groupby("Entity")["# of BTC"].diff().fillna("NEW")
    
    return df

df = load_data()

# ===== 3. Sidebar Filters =====
st.sidebar.header("Filters")

dates = sorted(df["Scrape Date"].unique(), reverse=True)
selected_date = st.sidebar.selectbox("Select Date", dates, index=0)

search_term = st.sidebar.text_input("Search Company").strip()

# ===== 4. Filter Data =====
df_selected = df[df["Scrape Date"] == selected_date]

if search_term:
    df_selected = df_selected[df_selected["Entity"].str.contains(search_term, case=False, na=False)]

df_selected = df_selected.sort_values("# of BTC", ascending=False)

# ===== 5. Show Metrics =====
total_btc = df_selected["# of BTC"].sum()
total_percent_21m = df_selected["% of 21m"].sum()

col1, col2 = st.columns(2)
col1.metric("Total BTC", f"{total_btc:,.0f}")
col2.metric("Total % of 21M BTC", f"{total_percent_21m:.3f}%")

# ===== 6. Show Table =====
st.subheader(f"Companies on {selected_date.date()}")

display_cols = ["Entity", "Symbol:Exchange", "# of BTC", "% of 21m", "Scrape Date", "BTC Change"]

st.dataframe(df_selected[display_cols], use_container_width=True)

# ===== 7. Historical Charts =====
st.subheader("📈 Total BTC Over Time")
historical_totals = df.groupby("Scrape Date")["# of BTC"].sum().reset_index()
st.line_chart(historical_totals.set_index("Scrape Date"))

st.subheader("📊 Total % of 21M Over Time")
historical_percent = df.groupby("Scrape Date")["% of 21m"].sum().reset_index()
st.line_chart(historical_percent.set_index("Scrape Date"))

