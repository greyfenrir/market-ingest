"""Samples how much disk the archive occupies and publishes it to Netdata.

Run on a timer, once, from a single place - deliberately NOT inside the
collectors. Every collector would report the same database-wide totals, and
nine of them would report it nine times.

Per-exchange bytes are apportioned from row counts rather than measured
directly: ClickHouse reports size per part, not per column value, and
partitioning by exchange purely to make a chart prettier would be a poor trade.
Row counts by exchange are cheap because `exchange` is the leading column of
every ORDER BY, so the estimate costs almost nothing and is accurate to the
extent that rows of one exchange compress like another's.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
import os
import sys
import urllib.request
from pathlib import Path
try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    import tomli as tomllib

from .metrics import Statsd

log = logging.getLogger("storage_probe")

TABLES = ("mark_price", "trades", "liquidations", "open_interest", "book_agg")


def ch_query(url: str, user: str, password: str, sql: str) -> list[dict]:
    req = urllib.request.Request(
        url,
        data=(sql + " FORMAT JSONEachRow").encode(),
        headers={
            "Authorization": "Basic " + base64.b64encode(
                f"{user}:{password}".encode()).decode(),
        },
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        body = r.read().decode()
    return [json.loads(line) for line in body.splitlines() if line.strip()]


async def amain(cfg: dict) -> int:
    ch = cfg.get("clickhouse", {})
    url = ch.get("url", "http://127.0.0.1:8123")
    user = ch.get("user", "ingest")
    password = os.environ.get("CH_PASSWORD", "")
    if not password:
        log.error("CH_PASSWORD is not set")
        return 2

    m = cfg.get("metrics", {})
    sd = Statsd(
        host=m.get("statsd_host", "127.0.0.1"),
        port=int(m.get("statsd_port", 8125)),
        prefix=m.get("prefix", "market_ingest"),
        enabled=bool(m.get("enabled", True)),
    )
    await sd.start()

    # Size per table: metadata only, no data is read.
    parts = ch_query(url, user, password, """
        SELECT table,
               sum(rows)          AS rows,
               sum(bytes_on_disk) AS bytes
        FROM system.parts
        WHERE database = 'market' AND active
        GROUP BY table
    """)
    by_table = {p["table"]: (int(p["rows"]), int(p["bytes"])) for p in parts}

    total_bytes = 0
    for table, (rows, nbytes) in by_table.items():
        sd.gauge(f"disk_bytes.table.{table}", nbytes)
        sd.gauge(f"disk_rows.table.{table}", rows)
        total_bytes += nbytes
    sd.gauge("disk_bytes.total", total_bytes)

    # Rows per exchange, then bytes apportioned by share of rows.
    per_exchange: dict[str, dict[str, int]] = {}
    for table in TABLES:
        if table not in by_table:
            continue
        try:
            rows = ch_query(url, user, password,
                            f"SELECT exchange, count() AS n FROM market.{table} GROUP BY exchange")
        except Exception as exc:  # noqa: BLE001
            log.warning("cannot count %s by exchange: %s", table, exc)
            continue
        table_rows, table_bytes = by_table[table]
        for row in rows:
            ex = row["exchange"]
            n = int(row["n"])
            bucket = per_exchange.setdefault(ex, {"rows": 0, "bytes": 0})
            bucket["rows"] += n
            if table_rows:
                bucket["bytes"] += int(table_bytes * n / table_rows)

    for ex, v in per_exchange.items():
        sd.gauge(f"disk_bytes.exchange.{ex}", v["bytes"])
        sd.gauge(f"disk_rows.exchange.{ex}", v["rows"])

    await sd.stop()   # stop() flushes what is pending
    log.info("published %d tables, %d exchanges, %.1f MiB total",
             len(by_table), len(per_exchange), total_bytes / 1048576)
    return 0


def main() -> int:
    p = argparse.ArgumentParser(prog="storage_probe")
    p.add_argument("--config", default="/etc/market-ingest/config.toml", type=Path)
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    return asyncio.run(amain(tomllib.loads(args.config.read_text())))


if __name__ == "__main__":
    sys.exit(main())
