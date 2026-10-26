"""Re-parse stored Target pages with a new parser. No new requests are sent."""

import gzip
import re
from decimal import Decimal

import duckdb

EXPECTED_MODEL = "1000XM5"


def parse_target(html: str):
    """Target showed the price only in server-rendered HTML, in US dollars."""
    price = re.search(r'data-test="product-price"[^>]*>\s*\$([\d,]+\.\d{2})', html)
    title = re.search(r'<h1[^>]*data-test="product-title"[^>]*>([^<]+)', html)
    if not (price and title):
        return None
    name = title.group(1).strip()
    if EXPECTED_MODEL not in re.sub(r"[^0-9A-Z]", "", name.upper()):
        return None
    return name, int(Decimal(price.group(1).replace(",", "")) * 100)


con = duckdb.connect("web_prices.duckdb")
raw_paths = [r[0] for r in con.sql(
    "SELECT raw_path FROM extract_log "
    "WHERE source = 'target' AND reason = 'no_jsonld_product' ORDER BY captured_at"
).fetchall()]
recovered = 0
for raw_path in raw_paths:
    html = gzip.decompress(open(raw_path, "rb").read()).decode("utf-8", "replace")
    parsed = parse_target(html)
    if parsed is None:
        continue
    name, price_minor = parsed
    # Copy the original capture time from the log: the row describes that moment
    # A replayed row never replaces a newer capture of the same day
    con.execute(
        """INSERT INTO prices
           SELECT source, url, country, timezone('UTC', captured_at)::DATE,
                  captured_at, ?, ?, 'USD', raw_path
           FROM extract_log WHERE raw_path = ?
           ON CONFLICT (url, country, captured_on) DO UPDATE SET
               captured_at = excluded.captured_at, title = excluded.title,
               price_minor = excluded.price_minor, raw_path = excluded.raw_path
           WHERE excluded.captured_at > prices.captured_at""",
        [name, price_minor, raw_path],
    )
    recovered += 1
print(f"re-parsed={len(raw_paths)} recovered={recovered}")
print(con.sql(
    "SELECT source, captured_at, price_minor, currency, title[:40] AS title "
    "FROM prices WHERE source = 'target'"
))
