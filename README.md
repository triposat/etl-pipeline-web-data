# Web ETL pipeline examples

Code for the Bright Data guide [What Is an ETL Pipeline? A Guide for Web Data Sources](https://brightdata.com/blog/proxy-101/etl-pipeline).

| File | What it does |
|---|---|
| `web_price_etl.py` | Extracts product pages through the Bright Data Web Unlocker API, stores each raw page, validates it, and upserts prices into DuckDB |
| `checks.sql` | Reject-rate and freshness queries for monitoring |
| `reparse_target.py` | Re-parses stored Target pages with a new parser, without new requests |
| `backfill_and_increment.py` | Loads records from a Bright Data dataset, then adds new scrapes from the Web Scraper API |

## Setup

You need Python 3.11 or later and a Bright Data account with a Web Unlocker zone.

```bash
python -m pip install -r requirements.txt
export BRIGHTDATA_API_KEY="your-api-key"
export BRIGHTDATA_ZONE="web_unlocker"  # Your Web Unlocker zone name
```

## Run

```bash
python web_price_etl.py
python reparse_target.py
python backfill_and_increment.py
```

Run the monitoring queries in `checks.sql` from Python:

```bash
python - <<'EOF'
import duckdb
con = duckdb.connect("web_prices.duckdb")
for query in open("checks.sql").read().split(";"):
    if query.strip():
        print(con.sql(query))
EOF
```

The guide explains each step and shows the expected output.
