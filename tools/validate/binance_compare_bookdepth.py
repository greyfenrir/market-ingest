"""Compare market.book_agg (Binance) with Binance's own bookDepth archive.

The archive (data.binance.vision, futures/um/daily/bookDepth) holds, every
~30 s, the cumulative depth from mid within +-0.2/1/2/3/4/5 % per side: `depth`
in the base coin and `notional` in USD. The file for day D appears around
08 UTC on D+1. book_agg has a row a second, so every archive instant has a row
to compare with, at the same second (--offset shifts it).

Two ratios per band and side, ours / archive:
- qty: our depth_*_usd / mid against `depth`. The cleanest comparison: our
  USD figure is quantity x mark, mark and mid differ by the basis only;
- usd: our depth_*_usd against `notional`. The archive values every level at
  its own price, we at the mark, so on wide bands bids come out ~0.5 % high and
  asks ~0.5 % low by construction, not because the book differs.

Only rows in state ok count. The main table also drops books younger than
--min-age seconds: on BTC a book needs about 2 h after a start or reconnect
until deltas have filled the levels beyond the 1000-level snapshot (ETH about
30 min); the warm-up table shows that by age.

    python tools/validate/binance_compare_bookdepth.py --date 2026-11-05 \\
        --ch 'http://172.16.0.106:8123/?user=ro'
"""

from __future__ import annotations

import argparse
import calendar
import csv
import io
import time
import zipfile
from collections import defaultdict
from pathlib import Path

from _common import (AGE_BUCKETS, BAND_COL, HEADER, add_ch_args, age_bucket, ch_query, ch_url,
                     download, fmt, summary)

ARCHIVE = "https://data.binance.vision/data/futures/um/daily/bookDepth/{s}/{s}-bookDepth-{d}.zip"
COMPARED = (0.2, 1.0, 2.0)     # bands both sides have (the archive has no 0.5 %)
DEFAULT_SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT")


def load_archive(path: Path) -> dict[int, dict[float, tuple[float, float]]]:
    """unix second -> {signed percentage: (depth, notional)}"""
    out: dict[int, dict] = defaultdict(dict)
    with zipfile.ZipFile(path) as z:
        text = z.read(z.namelist()[0]).decode()
    for r in csv.DictReader(io.StringIO(text)):
        t = calendar.timegm(time.strptime(r["timestamp"], "%Y-%m-%d %H:%M:%S"))
        out[t][float(r["percentage"])] = (float(r["depth"]), float(r["notional"]))
    return out


def load_ours(url: str, exchange: str, symbol: str, day: str) -> dict[int, dict]:
    cols = ", ".join(f"depth_{s}_usd_{BAND_COL[b]}" for s in ("bid", "ask") for b in COMPARED)
    rows = ch_query(url, f"""
        SELECT toUnixTimestamp(event_time) AS t, state, book_age_s, mid, {cols},
               dateDiff('millisecond', last_update_time, toDateTime64(event_time, 3, 'UTC')) AS lag_ms
        FROM market.book_agg
        WHERE exchange = '{exchange}' AND symbol = '{symbol}'
          AND event_time >= toDateTime('{day} 00:00:00', 'UTC') - INTERVAL 1 MINUTE
          AND event_time <  toDateTime('{day} 00:00:00', 'UTC') + INTERVAL 1 DAY + INTERVAL 1 MINUTE""")
    ours: dict[int, dict] = {}
    for r in rows:
        ours.setdefault(int(r["t"]), r)     # a duplicate row keeps the first
    return ours


def compare(arch: dict, ours: dict, offset: int, min_age: int,
            max_lag_ms: int | None = None) -> tuple[dict, dict, dict]:
    counts = defaultdict(int)
    main = defaultdict(list)                 # (metric, band, side) -> ratios
    warm = defaultdict(list)                 # (metric, band, bucket) -> ratios
    for t, bands in sorted(arch.items()):
        counts["archive"] += 1
        r = ours.get(t + offset)
        if r is None:
            counts["no row"] += 1
            continue
        if r["state"] != "ok":
            counts[f"state {r['state']}"] += 1
            continue
        if max_lag_ms is not None and int(r["lag_ms"]) > max_lag_ms:
            counts["ok, lagging"] += 1       # the book was seconds behind the venue
            continue
        counts["ok"] += 1
        age, mid = int(r["book_age_s"]), float(r["mid"])
        old = age >= min_age
        counts["ok, old enough"] += old
        for b in COMPARED:
            for side, sign in (("bid", -1), ("ask", 1)):
                ref = bands.get(sign * b)
                usd = float(r[f"depth_{side}_usd_{BAND_COL[b]}"])
                if not ref or ref[0] <= 0 or ref[1] <= 0 or mid <= 0:
                    continue
                q, u = usd / mid / ref[0], usd / ref[1]
                for metric, x in (("qty", q), ("usd", u)):
                    if old:
                        main[metric, b, side].append(x)
                    warm[metric, b, age_bucket(age)].append(x)
    return counts, main, warm


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--date", required=True, nargs="+", help="UTC day(s), YYYY-MM-DD")
    p.add_argument("--symbols", nargs="+", default=list(DEFAULT_SYMBOLS))
    add_ch_args(p)
    p.add_argument("--exchange", default="binance_um")
    p.add_argument("--cache", type=Path, default=Path.home() / ".cache" / "market-ingest" / "bookDepth")
    p.add_argument("--min-age", type=int, default=1800,
                   help="book_age_s below this is left out of the main table (default 1800)")
    p.add_argument("--offset", type=int, default=0,
                   help="compare the archive instant T with our row at T+offset seconds")
    p.add_argument("--max-lag-ms", type=int,
                   help="leave out rows whose last applied update is older than this: a state "
                        "ok book can still be seconds behind when delivery stalls")
    a = p.parse_args()
    url = ch_url(a.ch, a.ch_cmd)
    for day in a.date:
        for sym in a.symbols:
            src = ARCHIVE.format(s=sym, d=day)
            path = download(src, a.cache / Path(src).name, src + ".CHECKSUM")
            counts, main_t, warm = compare(load_archive(path), load_ours(url, a.exchange, sym, day),
                                           a.offset, a.min_age, a.max_lag_ms)
            print(f"\n== {sym} {day}  " + ", ".join(f"{k} {v}" for k, v in counts.items()))
            print(f"ours / archive, state ok, book_age_s >= {a.min_age}")
            print(f"{'band':>5} {'side':4} | qty {HEADER} | usd {HEADER}")
            for b in COMPARED:
                for side in ("bid", "ask"):
                    print(f"{b:>4}% {side:4} | qty {fmt(summary(main_t['qty', b, side]))}"
                          f" | usd {fmt(summary(main_t['usd', b, side]))}")
            print("warm-up: median qty ratio by book age, both sides")
            print(f"{'band':>5} " + " ".join(f"{name:>14}" for _, _, name in AGE_BUCKETS))
            for b in (1.0, 2.0):
                cells = []
                for _, _, name in AGE_BUCKETS:
                    st = summary(warm["qty", b, name])
                    cells.append(f"{st['median']:.3f} n={st['n']:<5}" if st["n"] else f"{'-':>14}")
                print(f"{b:>4}% " + " ".join(f"{c:>14}" for c in cells))


if __name__ == "__main__":
    main()
