"""Extract product pages, keep the raw HTML, validate, transform, and upsert."""

import datetime as dt
import gzip
import hashlib
import json
import os
import re
import time
from collections import deque
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path

import duckdb
import requests

API_URL = "https://api.brightdata.com/request"
API_KEY = os.environ["BRIGHTDATA_API_KEY"]
ZONE = os.environ.get("BRIGHTDATA_ZONE", "web_unlocker")
RAW_DIR = Path("raw")
EXPECTED_MODEL = "1000XM5"  # Every page must name the item we asked for

# ISO 4217 currencies whose minor unit is not 2 (SIX list). Other codes use 2.
MINOR_UNITS = {
    "BIF": 0, "CLP": 0, "DJF": 0, "GNF": 0, "ISK": 0, "JPY": 0, "KMF": 0,
    "KRW": 0, "PYG": 0, "RWF": 0, "UGX": 0, "UYI": 0, "VND": 0, "VUV": 0,
    "XAF": 0, "XOF": 0, "XPF": 0, "BHD": 3, "IQD": 3, "JOD": 3, "KWD": 3,
    "LYD": 3, "OMR": 3, "TND": 3, "CLF": 4, "UYW": 4,
}  # fmt: skip

SOURCES = {
    "bestbuy": "https://www.bestbuy.com/site/sony-wh-1000xm5-wireless-noise-canceling-over-the-ear-headphones-black/6505727.p?skuId=6505727",
    "sony": "https://electronics.sony.com/audio/headphones/headband/p/wh1000xm5-b",
    "ebay": "https://www.ebay.com/itm/318849109603",
    "target": "https://www.target.com/p/sony-wh-1000xm5-bluetooth-wireless-noise-canceling-headphones-black/-/A-86314264",
}


def extract(url: str, country: str, attempts: int = 4):
    """Return (target_status, raw_bytes, text). 0: no response, below 0: bad request."""
    status, raw, text = 0, b"", ""
    for attempt in range(attempts):
        try:
            resp = requests.post(
                API_URL,
                json={"zone": ZONE, "url": url, "format": "raw", "country": country},
                headers={"Authorization": f"Bearer {API_KEY}"},
                timeout=180,
            )
        except requests.RequestException:  # Timeout or connection error
            resp = None
        if resp is not None and resp.status_code in (401, 407):
            resp.raise_for_status()  # A bad API key or zone: retrying will not help
        if resp is not None and resp.status_code in (400, 403):
            return -resp.status_code, b"", ""  # A request error for this URL: log it
        if resp is not None and resp.status_code == 200:
            # Read the target site's own status from the header.
            header = resp.headers.get("x-brd-status-code")
            status = int(header) if header else None
            raw, text = resp.content, resp.text
            if status is not None and status != 429 and status < 500 and raw:
                return status, raw, text
        if attempt < attempts - 1:  # 429, 5xx, timeout, or empty body: back off
            time.sleep(5 * 2**attempt)
    return status, raw, text


def land_raw(raw: bytes, source: str, captured_at: dt.datetime) -> str:
    """Store the response bytes, gzipped. A website does not serve past versions."""
    digest = hashlib.sha256(raw).hexdigest()
    day = captured_at.strftime("%Y-%m-%d")
    path = RAW_DIR / f"source={source}" / f"dt={day}" / f"{digest}.html.gz"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(gzip.compress(raw))
    return str(path)


def is_type(node: dict, name: str) -> bool:
    kinds = node.get("@type")
    kinds = kinds if isinstance(kinds, list) else [kinds]
    return any(isinstance(k, str) and k.rsplit("/", 1)[-1] == name for k in kinds)


def product_from_jsonld(html: str) -> dict | None:
    """Return the first schema.org Product in document order, if the page has one."""
    pattern = r"<script[^>]*application/ld\+json[^>]*>(.*?)</script>"
    for block in re.findall(pattern, html, re.S | re.I):
        try:
            queue = deque([json.loads(block)])
        except json.JSONDecodeError:
            continue
        while queue:  # Breadth-first, so the main Product wins over nested ones
            node = queue.popleft()
            if isinstance(node, list):
                queue.extend(node)
            elif isinstance(node, dict):
                if is_type(node, "Product"):
                    return node
                queue.extend(v for v in node.values() if isinstance(v, (dict, list)))
    return None


def transform(source, url, country, status, html, captured_at, raw_path):
    """Return (row, None) for a valid record or (None, reason) for a rejected one."""
    if status is None:
        return None, "no_target_status"
    if status < 0:
        return None, f"api_status_{-status}"
    if status == 0:
        return None, "no_response"
    if status != 200:
        return None, f"target_status_{status}"
    product = product_from_jsonld(html)
    if product is None:
        return None, "no_jsonld_product"
    name = product.get("name") or ""
    if EXPECTED_MODEL not in re.sub(r"[^0-9A-Z]", "", name.upper()):
        return None, "item_mismatch"
    offers = product.get("offers") or {}
    offer = offers[0] if isinstance(offers, list) and offers else offers
    price = offer.get("price") or offer.get("lowPrice")
    currency = offer.get("priceCurrency") or ""
    if price is None or not re.fullmatch(r"[A-Z]{3}", currency):
        return None, "no_price_or_currency"
    try:  # schema.org prices use "." as the decimal mark and no symbols
        amount = Decimal(str(price))
    except InvalidOperation:
        return None, "unparseable_price"
    exponent = MINOR_UNITS.get(currency, 2)
    price_minor = int((amount * 10**exponent).quantize(Decimal(1), ROUND_HALF_UP))
    if price_minor <= 0:
        return None, "non_positive_price"
    return {
        "source": source,
        "url": url,
        "country": country,
        "captured_on": captured_at.date(),  # UTC date
        "captured_at": captured_at,
        "title": name,
        "price_minor": price_minor,
        "currency": currency,
        "raw_path": raw_path,
    }, None


UPSERT = """
    INSERT INTO prices VALUES (?,?,?,?,?,?,?,?,?)
    ON CONFLICT (url, country, captured_on) DO UPDATE SET
        source = excluded.source, captured_at = excluded.captured_at,
        title = excluded.title, price_minor = excluded.price_minor,
        currency = excluded.currency, raw_path = excluded.raw_path
    WHERE excluded.captured_at > prices.captured_at"""


def load(con, rows, attempts):
    con.execute("""
        CREATE TABLE IF NOT EXISTS prices (
            source TEXT, url TEXT, country TEXT,
            captured_on DATE, captured_at TIMESTAMPTZ,
            title TEXT, price_minor BIGINT, currency TEXT, raw_path TEXT,
            PRIMARY KEY (url, country, captured_on))""")
    con.execute("""
        CREATE TABLE IF NOT EXISTS extract_log (
            source TEXT, url TEXT, country TEXT, captured_at TIMESTAMPTZ,
            target_status INTEGER, reason TEXT, raw_path TEXT)""")
    # Upsert: the newest capture of a UTC day wins, whatever order rows arrive in
    for r in rows:
        con.execute(UPSERT, list(r.values()))
    # Append every attempt. reason is NULL when the row loaded
    con.executemany("INSERT INTO extract_log VALUES (?,?,?,?,?,?,?)", attempts)


def run(country: str = "us"):
    con = duckdb.connect("web_prices.duckdb")
    rows, attempts = [], []
    for source, url in SOURCES.items():
        captured_at = dt.datetime.now(dt.timezone.utc)
        status, raw, html = extract(url, country)
        raw_path = land_raw(raw, source, captured_at) if raw else None
        row, reason = transform(
            source, url, country, status, html, captured_at, raw_path
        )
        if row:
            rows.append(row)
        attempts.append([source, url, country, captured_at, status, reason, raw_path])
    load(con, rows, attempts)
    print(f"loaded={len(rows)} rejected={len(attempts) - len(rows)}")
    print(
        con.sql(
            "SELECT source, country, price_minor, currency, "
            "title[:40] AS title FROM prices ORDER BY source"
        )
    )
    print(
        con.sql(
            "SELECT source, reason, count(*) AS n FROM extract_log "
            "WHERE reason IS NOT NULL GROUP BY ALL ORDER BY source"
        )
    )


if __name__ == "__main__":
    run()
