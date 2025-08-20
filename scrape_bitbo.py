import os
import re
import requests
import pandas as pd
from io import StringIO
from datetime import datetime
from dotenv import load_dotenv
from firecrawl import FirecrawlApp

# ===== Load environment =====
load_dotenv()
API_KEY = os.getenv("FIRECRAWL_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

CSV_PATH = "bitbo_historical.csv"

# ===== Telegram helpers =====
def send_telegram_message(token: str, chat_id: str, text: str, parse_mode: str = "HTML") -> None:
    """Send one message to Telegram."""
    if not token or not chat_id:
        # Not configured; silently skip.
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }
    try:
        r = requests.post(url, json=payload, timeout=20)
        r.raise_for_status()
    except Exception as e:
        # Avoid crashing the whole job if Telegram is down/misconfigured
        print(f"[warn] Telegram send failed: {e}")

def build_change_message(changes_df: pd.DataFrame, as_of_date) -> str:
    """
    Build a single summary message listing companies with BTC changes.
    Uses HTML formatting; safe for Telegram parse_mode="HTML".
    """
    header = f"📣 <b>Public Company BTC Holdings Update — {as_of_date:%Y-%m-%d}</b>\n"
    lines = []
    # Sort biggest absolute movers first
    tmp = changes_df.copy()
    tmp["abs_change"] = tmp["BTC Change"].abs()
    tmp = tmp.sort_values("abs_change", ascending=False)

    for _, r in tmp.iterrows():
        entity = r["Entity"]
        change = r["BTC Change"]
        total = r["# of BTC"]
        value_change = r.get("Value Change", None)

        direction = "bought ➕" if change > 0 else "sold ➖"
        vc_part = f" (≈ ${value_change:,.0f})" if pd.notna(value_change) else ""
        lines.append(
            f"• <b>{entity}</b> {direction} {change:,.0f} BTC. "
            f"New total: {total:,.0f} BTC."
        )

    net = changes_df["BTC Change"].sum()
    footer = f"\nNet change (these movers): <b>{net:,.0f} BTC</b>."
    return header + "\n".join(lines) + footer

# ===== Scraper & updater =====
def scrape_bitbo_public_btc(api_key: str) -> pd.DataFrame:
    """
    Scrapes the 'Public Companies that Own Bitcoin' table from Bitbo,
    cleans it, and returns a DataFrame with today's scrape date (UTC).
    """
    app = FirecrawlApp(api_key=api_key)
    scrape_result = app.scrape_url(
        "https://bitbo.io/treasuries/",
        formats=["markdown", "html"]
    )
    markdown_text = scrape_result.markdown

    # Extract the specific table from the markdown
    pattern = r"## Public Companies that Own Bitcoin\s*\n\n(\|.+?)(?=\n\n##|\Z)"
    match = re.search(pattern, markdown_text, re.S)
    if not match:
        raise ValueError("Could not find the 'Public Companies that Own Bitcoin' table in the markdown.")

    table_md = match.group(1)

    # Parse the markdown table
    df = pd.read_csv(StringIO(table_md), sep="|", engine="python")
    df = df.dropna(how="all", axis=1)  # remove empty columns
    df = df.rename(columns=lambda x: x.strip())  # strip whitespace
    df = df.drop(index=0)  # remove markdown separator row

    # Helper to strip markdown
    def strip_markdown(s):
        if pd.isna(s):
            return s
        s = re.sub(r"!\[.*?\]\(.*?\)", "", s)  # images
        s = re.sub(r"\[(.*?)\]\(.*?\)", r"\1", s)  # links
        return s.strip()

    for col in df.columns:
        df[col] = df[col].map(strip_markdown)

    # Rename columns to stable names
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
    df["Value Today"] = (
        df["Value Today"]
        .str.replace(",", "", regex=False)
        .replace(r"[^0-9.]", "", regex=True)
        .astype(float)
    )
    df["% of 21m"] = df["% of 21m"].str.replace("%", "", regex=False).astype(float)

    # Add scrape date (UTC date to match your pipeline)
    df["Scrape Date"] = datetime.utcnow().date()
    return df

def update_historical_data(api_key: str, csv_path: str, min_abs_btc_to_alert: float = 1.0):
    """
    Scrapes today's data and appends it to a historical CSV.
    If prior data exists, calculates daily changes and sends a Telegram alert
    when meaningful changes are detected (|Δ BTC| >= min_abs_btc_to_alert).
    """
    today_df = scrape_bitbo_public_btc(api_key)

    # If file exists, compute diffs vs last scrape date
    if os.path.exists(csv_path):
        hist_df = pd.read_csv(csv_path)
        hist_df["Scrape Date"] = pd.to_datetime(hist_df["Scrape Date"]).dt.date

        last_date = max(hist_df["Scrape Date"])
        today_date = today_df["Scrape Date"].iloc[0]

        if today_date == last_date:
            print("Data already scraped for today. No update made.")
            return hist_df  # avoid duplicate alerts

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

        # Persist the new rows
        hist_df = pd.concat([hist_df, merged], ignore_index=True)
        hist_df.to_csv(csv_path, index=False)
        print(f"Updated historical data saved to {csv_path}")

        # ===== Telegram alert logic =====
        # Only alert for rows with real changes (not NaN and not zero)
        changes = merged[
            merged["BTC Change"].notna() & (merged["BTC Change"].abs() >= float(min_abs_btc_to_alert))
        ].copy()

        if not changes.empty:
            msg = build_change_message(changes, today_date)
            send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, msg)
        else:
            print("No meaningful BTC changes; no Telegram alert sent.")

        return hist_df

    else:
        # First run: just create the file; no alert (no previous baseline)
        merged = today_df.copy()
        merged["BTC Change"] = None
        merged["Value Change"] = None
        merged.to_csv(csv_path, index=False)
        print(f"Historical file created at {csv_path}")
        return merged

if __name__ == "__main__":
    df_hist = update_historical_data(API_KEY, CSV_PATH, min_abs_btc_to_alert=1.0)
    # Print tail for CI logs
    #print(df_hist.tail())
