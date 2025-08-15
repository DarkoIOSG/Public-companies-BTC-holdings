# Public-companies-BTC-holdings

.
├── scrape_bitbo.py                # Scrapes & parses table, updates history & derived files
├── streamlit_app.py               # Streamlit dashboard
├── requirements.txt
├── .env.example                   # Template for local dev (FIRECRAWL_API_KEY)
├── README.md
└── .github/
    └── workflows/
        └── daily_scrape.yml       # GitHub Actions to run the scraper daily
        
1. Scrape Bitbo’s Public Companies that Own Bitcoin table daily using Firecrawl

2. Append snapshots to a history file with a Scrape Date

3. Calculate daily per-company BTC changes and totals

4. Visualize it in a simple Streamlit dashboard

5. Automate the scrape with GitHub Actions

