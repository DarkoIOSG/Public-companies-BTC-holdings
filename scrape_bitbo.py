import os
import pandas as pd
from datetime import datetime
from firecrawl import FirecrawlApp  # pip install firecrawl
from dotenv import load_dotenv

load_dotenv()

# ===== 1. Config =====
FIRECRAWL_API_KEY = os.getenv("FIRECRAWL_API_KEY")
CSV_PATH = "btc_holdings_history.csv"

# ===== 2. Scrape today's data =====
def scrape_bitbo():
    app = FirecrawlApp(api_key=FIRECRAWL_API_KEY)
    scrape_result = app.scrape_url(
        "https://bitbo.io/public-companies-bitcoin/",
        params={"formats": ["markdown"]}
    )

    md_text = scrape_result["markdown"]

    # Extract the table for public companies
    lines = md_text.split("\n")
    start_idx = next(i for i, line in enumerate(lines) if "Public Companies that Own Bitcoin" in line)
    table_lines = []
    for line in lines[start_idx:]:
        if "|" in line and "Entity" not in line:
            table_lines.append(line)
        if "**Totals:**" in line:
            table_lines.append(line)
            break

    # Convert markdown table to DataFrame
    from io import StringIO
    table_str = "\n".join(["Entity|Country|Symbol:Exchange|Filings & Sources|# of BTC|Value Today|% of 21m"] + table_lines)
    df = pd.read_csv(StringIO(table_str), sep="|")
    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)

    # Drop totals row for per-company view
    df_no_totals = df[~df["Entity"].str.contains("Totals", na=False)]

    # Add scrape date
    df_no_totals["Scrape Date"] = datetime.utcnow().strftime("%Y-%m-%d")

    # Convert BTC column to numeric
    df_no_totals["# of BTC"] = pd.to_numeric(df_no_totals["# of BTC"].str.replace(",", ""), errors="coerce")

    return df_no_totals


# ===== 3. Append to CSV =====
def append_to_history(new_data):
    if os.path.exists(CSV_PATH):
        old_data = pd.read_csv(CSV_PATH)
        combined = pd.concat([old_data, new_data], ignore_index=True)
    else:
        combined = new_data

    combined.to_csv(CSV_PATH, index=False)
    return combined


# ===== 4. Calculate daily changes =====
def calculate_changes(history_df):
    # Per-company daily BTC change
    company_changes = (
        history_df
        .sort_values(["Entity", "Scrape Date"])
        .groupby("Entity")
        .apply(lambda g: g.assign(Daily_Change=g["# of BTC"].diff()))
        .reset_index(drop=True)
    )
    company_changes.to_csv("company_daily_changes.csv", index=False)

    # Total BTC per day
    total_per_day = history_df.groupby("Scrape Date")["# of BTC"].sum().reset_index()
    total_per_day.rename(columns={"# of BTC": "Total_BTC"}, inplace=True)
    total_per_day.to_csv("daily_total_btc.csv", index=False)

    # Total BTC daily change
    total_per_day["Daily_Net_Change"] = total_per_day["Total_BTC"].diff()
    total_per_day.to_csv("daily_net_change.csv", index=False)

    return company_changes, total_per_day


if __name__ == "__main__":
    print("Scraping Bitbo data...")
    todays_data = scrape_bitbo()

    print(f"Appending {len(todays_data)} rows for {todays_data['Scrape Date'].iloc[0]}")
    history = append_to_history(todays_data)

    print("Calculating daily changes...")
    company_changes, total_changes = calculate_changes(history)

    print("Done.")
