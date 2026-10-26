"""Backfill from a Bright Data dataset, then add fresh scrapes with matching fields."""

import json
import os
import time

import duckdb
import requests

API = "https://api.brightdata.com"
HEADERS = {"Authorization": f"Bearer {os.environ['BRIGHTDATA_API_KEY']}"}
# Amazon products: the dataset and the scraper share this ID
DATASET_ID = "gd_l7q7dkf244hwjntr0"
ASINS = ["B09XS7JWHH", "B0863TXGM3"]


def download(status_url: str, file_url: str, deadline_s: int = 1800) -> list[dict]:
    """Poll a snapshot until it is ready, then download it as JSON."""
    deadline = time.monotonic() + deadline_s
    status = "unknown"
    while time.monotonic() < deadline:
        try:
            resp = requests.get(status_url, headers=HEADERS, timeout=60)
            resp.raise_for_status()
            status = resp.json()["status"]
        except requests.RequestException:  # A network blip should not end the job
            status = "unknown"
        if status == "ready":
            break
        if status in ("failed", "canceled"):
            raise RuntimeError(f"snapshot {status}")
        time.sleep(10)
    else:
        raise TimeoutError(f"snapshot not ready after {deadline_s} s: {status}")
    while True:
        resp = requests.get(file_url, headers=HEADERS, params={"format": "json"}, timeout=300)
        if resp.status_code != 202:  # 202 means the file is still being built
            break
        time.sleep(10)
    resp.raise_for_status()
    return resp.json()


# 1. Backfill: filter the pre-collected dataset instead of crawling the past yourself.
# An ASIN repeats across Amazon marketplaces, so filter by domain as well.
backfill_filter = {
    "operator": "and",
    "filters": [
        {"name": "asin", "operator": "in", "value": ASINS},
        {"name": "domain", "operator": "=", "value": "https://www.amazon.com/"},
    ],
}
resp = requests.post(
    f"{API}/datasets/filter",
    headers=HEADERS,
    json={"dataset_id": DATASET_ID, "filter": backfill_filter},
    timeout=60,
)
resp.raise_for_status()
snap = resp.json()["snapshot_id"]
backfill = download(
    f"{API}/datasets/snapshots/{snap}", f"{API}/datasets/snapshots/{snap}/download"
)

# 2. Increment: scrape the same products now.
# include_errors=true returns each failed input as a row with an error_code.
inputs = [{"url": f"https://www.amazon.com/dp/{a}"} for a in ASINS + ["B0BX2L8PBT"]]
resp = requests.post(
    f"{API}/datasets/v3/trigger",
    headers=HEADERS,
    json=inputs,
    params={"dataset_id": DATASET_ID, "include_errors": "true"},
    timeout=60,
)
resp.raise_for_status()
snap = resp.json()["snapshot_id"]
fresh = download(f"{API}/datasets/v3/progress/{snap}", f"{API}/datasets/v3/snapshot/{snap}")

# 3. Write each source to its own file, and keep failed inputs apart
files = {
    "dataset.json": backfill,
    "scraper.json": [r for r in fresh if "error_code" not in r],
    "dead.json": [r for r in fresh if "error_code" in r],
}
for name, rows in files.items():
    with open(name, "w") as f:
        json.dump(rows, f)

# 4. Append both sources to one history table. union_by_name lines up the columns
# when one file has fields the other lacks. Prices become integer cents (USD).
con = duckdb.connect("amazon.duckdb")
select = """
    SELECT * EXCLUDE (filename), filename AS _source, now() AS _loaded_at,
           CAST(round(final_price * 100) AS BIGINT) AS final_price_minor
    FROM read_json(['dataset.json', 'scraper.json'], union_by_name = true,
                   filename = true)"""
con.execute(f"CREATE TABLE IF NOT EXISTS products AS {select} LIMIT 0")
con.execute(f"INSERT INTO products BY NAME {select}")
print(con.sql(
    "SELECT _source, asin, final_price_minor, currency, reviews_count "
    "FROM products ORDER BY asin, _source"
))
if files["dead.json"]:
    print(con.sql("SELECT input.url AS url, error_code FROM read_json('dead.json')"))
