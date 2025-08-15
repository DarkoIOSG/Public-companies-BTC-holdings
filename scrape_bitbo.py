import pandas as pd
import re
from firecrawl import FirecrawlApp
from io import StringIO
from datetime import datetime
import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()
API_KEY = os.getenv("FIRECRAWL_API_KEY")


def scrape_bitbo_public_btc(api_key: str) -> pd.DataFrame:
    """
    Scrapes the 'Public Companies that Own Bitcoin' table from Bitbo,
    cleans it, and returns a DataFrame with today's scrape date.
    """
    app = FirecrawlApp(api_key=api_key)

    # Scrape Bitbo page
    scrape_result = app.scrape_url(
        'https://bitbo.io/treasuries/',
        formats=['markdown', 'html']
    )
    markdown_text = scrape_result.markdown

    # Extract only the "Public Companies that Own Bitcoin" table
    pattern = r"## Public Companies that Own Bitcoin\s*\n\n(\|.+?)(?=\n\n##|\Z)"
    match = re.search(pattern, markdown_text, re.S)
    if not match:
        raise ValueError("Could not find the 'Public Companies that Own Bitcoin' table in the markdown.")

    table_md = match.group(1)

    # Read into pandas
    df = pd.read_csv(StringIO(table_md), sep="|", engine="python")
    df = df.dropna(how="all", axis=1)  # remove empty columns
    df = df.rename(columns=lambda x: x.strip())  # strip whitespace
    df = df.drop(index=0)  # remove markdown separator row

    # Helper: strip markdown formatting
    def strip_markdown(s):
        if pd.isna(s):
            return s
        s = re.sub(r"!\[.*?\]\(.*?\)", "", s)  # remove images
        s = re.sub(r"\[(.*?)\]\(.*?\)", r"\1", s)  # remove links
        return s.strip()

    for col in df.columns:
        df[col] = df[col].map(strip_markdown)

    # Rename columns
    df.columns = [
        "Entity",
        "Country",
        "Symbol:Exchange",
        "Filings & Sources",
        "# of BTC",
        "Value Today",
        "% of 21m"
    ]

    # Convert numeric columns
    df["# of BTC"] = df["# of BTC"].str.replace(",", "", regex=False).astype(float)
    df["Value Today"] = df["Value Today"].str.replace(",", "", regex=False).replace(r"[^0-9.]", "", regex=True).astype(float)
    df["% of 21m"] = df["% of 21m"].str.replace("%", "", regex=False).astype(float)

    # Add scrape date
    df["Scrape Date"] = datetime.utcnow().date()

    return df


def update_historical_data(api_key: str, csv_path: str):
    """
    Scrapes today's data and appends it to a historical CSV,
    also calculates daily changes if previous day's data exists.
    """
    today_df = scrape_bitbo_public_btc(api_key)

    if os.path.exists(csv_path):
        hist_df = pd.read_csv(csv_path)
        hist_df["Scrape Date"] = pd.to_datetime(hist_df["Scrape Date"]).dt.date

        # Find last scrape date
        last_date = max(hist_df["Scrape Date"])

        if today_df["Scrape Date"].iloc[0] == last_date:
            print("Data already scraped for today. No update made.")
            return hist_df

        # Merge to calculate changes
        last_df = hist_df[hist_df["Scrape Date"] == last_date]
        merged = today_df.merge(
            last_df[["Entity", "# of BTC", "Value Today"]],
            on="Entity",
            suffixes=("", "_prev"),
            how="left"
        )

        merged["BTC Change"] = merged["# of BTC"] - merged["# of BTC_prev"]
        merged["Value Change"] = merged["Value Today"] - merged["Value Today_prev"]

        merged.drop(columns=["# of BTC_prev", "Value Today_prev"], inplace=True)

        hist_df = pd.concat([hist_df, merged], ignore_index=True)

    else:
        merged = today_df.copy()
        merged["BTC Change"] = None
        merged["Value Change"] = None
        hist_df = merged

    hist_df.to_csv(csv_path, index=False)
    print(f"Updated historical data saved to {csv_path}")
    return hist_df


if __name__ == "__main__":
    CSV_PATH = "bitbo_historical.csv"
    df_hist = update_historical_data(API_KEY, CSV_PATH)
    print(df_hist.tail())
