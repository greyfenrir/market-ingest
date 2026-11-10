"""Compare market.book_agg (Bybit) with Bybit's orderbook.200 archive.

The archive (quote-saver.bycsi.com/orderbook/linear/SYM/DAY_SYM_ob200.data.zip,
published at 00:00 UTC of the next day) is the orderbook.200 WebSocket feed as
recorded by Bybit: one JSON message a line, a snapshot at 00:00 and deltas
(u + 1 each) after it. The script rebuilds that book on its own - no collector
code, so a bug there cannot hide itself - and at every whole second takes the
cumulative depth from mid in each band, as book_agg does.

The archive book holds only the best 200 levels. A band is compared only where
the archive's 200th level is beyond it on that side; on BTC and ETH 200 levels
do not even reach 0.2 %, so this archive checks altcoins (SOL, XRP, ...) only.
The live self-check (bybit_selfcheck.py) covers the majors.

Ratios are ours / archive in the base coin: our depth_*_usd / mid against the
archive's quantity (our USD figure is quantity x mark). Only rows in state ok.

Which archive instant goes with our row for second T (--align):
- seq (default): our row records seq_last, Bybit's cross sequence of the last
  message applied; the archive's messages carry the same counter (it is shared
  by every depth and only grows). The archive state used is the one after its
  first message with seq >= ours. No clock is involved, so it holds on any
  host. (Its last message with seq <= ours lags by up to one 100 ms push and
  matches worse: XRP at 0.2 %, 70 % of seconds within 1 % instead of 88 %.);
- time: the state after every archive message stamped (ts) <= T s + --lag-ms.
  The right lag depends on the host's delivery delay: +30 ms on the test site,
  about -150 ms on production, where Bybit arrives ~200 ms later. A wrong one shows up
  as scatter (XRP at 0.2 %: 92 % of seconds within 1 % turn into 50-65 %).

    python tools/validate/bybit_compare_ob200.py --date 2026-11-05 --symbols SOLUSDT XRPUSDT \\
        --ch 'http://172.16.0.106:8123/?user=ro'
    # a recording from bybit_record_ob200.py instead of the archive:
    python tools/validate/bybit_compare_ob200.py --file rec/2026-11-06_SOLUSDT_ob200.data ...
"""

from __future__ import annotations

import argparse
import json
import re
import zipfile
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path

from _common import (BAND_COL, BANDS, HEADER, add_ch_args, ch_query, ch_url, download, fmt,
                     summary)

ARCHIVE = "https://quote-saver.bycsi.com/orderbook/linear/{s}/{d}_{s}_ob200.data.zip"
LEVELS = 200


class Book:
    def __init__(self) -> None:
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.u = 0
        self.ok = False

    def on(self, msg: dict) -> str | None:
        """Apply one message; a broken sequence returns why and waits for the
        next snapshot."""
        d = msg["data"]
        u = int(d["u"])
        if msg["type"] == "snapshot":
            self.bids = {float(p): float(q) for p, q in d["b"]}
            self.asks = {float(p): float(q) for p, q in d["a"]}
            self.ok = True
        elif not self.ok:
            return None
        elif u != self.u + 1:
            self.ok = False
            return f"u={u}, expected {self.u + 1}"
        else:
            for side, key in ((self.bids, "b"), (self.asks, "a")):
                for p, q in d[key]:
                    p, q = float(p), float(q)
                    if q == 0:
                        side.pop(p, None)
                    else:
                        side[p] = q
        self.u = u
        return None

    def figures(self) -> dict | None:
        """mid, reach per side (% from mid of the 200th level, inf when the
        side is shorter: then it is the whole side) and depth per band."""
        if not self.ok or not self.bids or not self.asks:
            return None
        bids = sorted(self.bids.items(), reverse=True)[:LEVELS]
        asks = sorted(self.asks.items())[:LEVELS]
        mid = (bids[0][0] + asks[0][0]) / 2
        if bids[0][0] >= asks[0][0]:
            return None
        out = {"mid": mid}
        for name, levels, dist in (("bid", bids, lambda p: mid - p), ("ask", asks, lambda p: p - mid)):
            out[f"reach_{name}"] = (dist(levels[-1][0]) / mid * 100
                                    if len(levels) >= LEVELS else float("inf"))
            for b in BANDS:
                lim = mid * b / 100
                out[f"{name}_{b}"] = sum(q for p, q in levels if dist(p) <= lim)
        return out


@contextmanager
def open_feed(path: Path):
    if path.suffix == ".zip":
        with zipfile.ZipFile(path) as z, z.open(z.namelist()[0]) as f:
            yield f
    else:
        with open(path, "rb") as f:
            yield f


def replay(path: Path, lag_ms: int) -> tuple[dict[int, dict], list[str]]:
    """Archive figures at every whole second the feed covers."""
    book, at, breaks = Book(), {}, []
    pending = None          # the next whole second to take, in ms
    with open_feed(path) as f:
        for line in f:
            msg = json.loads(line)
            if "data" not in msg or "type" not in msg:
                continue
            ts = int(msg["ts"])
            if pending is None:
                pending = (ts // 1000 + 1) * 1000
            while ts > pending + lag_ms:      # this message is past the instant
                fig = book.figures()
                if fig:
                    at[pending // 1000] = fig
                pending += 1000
            why = book.on(msg)
            if why:
                breaks.append(f"ts={ts} {why}")
    return at, breaks


def replay_by_seq(path: Path, ours: dict[int, dict],
                  pick: str = "ge") -> tuple[dict[int, dict], list[str]]:
    """Archive figures for each of our seconds, matched by Bybit's seq: the
    state after the archive's first message with seq >= that second's seq_last
    (pick "ge") or after its last one with seq <= it ("le"). A second without
    updates (seq_last 0) stands where the previous one did."""
    targets, carried = [], 0
    for t in sorted(ours):
        carried = int(ours[t]["seq_last"]) or carried
        if carried:
            targets.append((carried, t))
    targets.sort()
    book, at, breaks, i = Book(), {}, [], 0

    def take(below: int, inclusive: bool) -> None:
        """Hand the current book to every pending target up to `below`."""
        nonlocal i
        fig = book.figures() if book.ok else None
        while i < len(targets) and (targets[i][0] <= below if inclusive else targets[i][0] < below):
            if fig:
                at[targets[i][1]] = fig
            i += 1

    with open_feed(path) as f:
        for line in f:
            msg = json.loads(line)
            if "data" not in msg or "type" not in msg:
                continue
            seq = int(msg["data"].get("seq") or 0)
            if pick == "le" and seq and i < len(targets) and targets[i][0] < seq:
                take(seq, inclusive=False)
            why = book.on(msg)
            if why:
                breaks.append(f"seq={seq} {why}")
            if pick == "ge" and seq and i < len(targets) and targets[i][0] <= seq:
                take(seq, inclusive=True)
    return at, breaks


def feed_range(path: Path) -> tuple[int, int]:
    """First and last whole second the feed covers."""
    first = last = None
    with open_feed(path) as f:
        for line in f:
            if b'"ts"' not in line[:200]:
                continue
            ts = int(json.loads(line)["ts"]) // 1000
            first = ts if first is None else first
            last = ts
    return first or 0, last or 0


def load_ours(url: str, symbol: str, t0: int, t1: int) -> dict[int, dict]:
    cols = ", ".join(f"depth_{s}_usd_{BAND_COL[b]}" for s in ("bid", "ask") for b in BANDS)
    rows = ch_query(url, f"""
        SELECT toUnixTimestamp(event_time) AS t, state, mid, covered_bid_pct, covered_ask_pct,
               seq_last, {cols}
        FROM market.book_agg
        WHERE exchange = 'bybit_linear' AND symbol = '{symbol}'
          AND event_time BETWEEN toDateTime({t0}, 'UTC') AND toDateTime({t1}, 'UTC')""")
    ours: dict[int, dict] = {}
    for r in rows:
        ours.setdefault(int(r["t"]), r)
    return ours


def report(symbol: str, label: str, arch: dict, ours: dict, breaks: list[str]) -> None:
    counts = defaultdict(int)
    ratios = defaultdict(list)
    mid_bps = []
    for t, a in sorted(arch.items()):
        counts["archive s"] += 1
        r = ours.get(t)
        if r is None:
            counts["no row"] += 1
            continue
        if r["state"] != "ok":
            counts[f"state {r['state']}"] += 1
            continue
        counts["ok"] += 1
        mid = float(r["mid"])
        mid_bps.append(abs(mid / a["mid"] - 1) * 1e4)
        for b in BANDS:
            for side in ("bid", "ask"):
                if a[f"reach_{side}"] < b or float(r[f"covered_{side}_pct"]) < b:
                    continue          # the archive's 200 levels, or our book, end inside it
                ref = a[f"{side}_{b}"]
                if ref > 0:
                    ratios[b, side].append(float(r[f"depth_{side}_usd_{BAND_COL[b]}"]) / mid / ref)
    reach = sorted(min(a["reach_bid"], a["reach_ask"]) for a in arch.values())
    print(f"\n== {symbol} {label}  " + ", ".join(f"{k} {v}" for k, v in counts.items())
          + f", sequence breaks {len(breaks)}")
    if reach:
        print(f"archive reach (200th level, nearer side): median {reach[len(reach) // 2]:.2f} %")
    if mid_bps:
        s = sorted(mid_bps)
        print(f"|mid ours - archive|: median {s[len(s) // 2]:.2f} bp, p99 {s[int(.99 * len(s))]:.2f} bp")
    print(f"{'band':>5} {'side':4} | qty {HEADER}")
    for b in BANDS:
        for side in ("bid", "ask"):
            print(f"{b:>4}% {side:4} | qty {fmt(summary(ratios[b, side]))}")
    for x in breaks[:5]:
        print("  break:", x)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--date", nargs="+", help="UTC day(s) of the archive, YYYY-MM-DD")
    p.add_argument("--file", type=Path, nargs="+", help="local .data or .zip files instead")
    p.add_argument("--symbols", nargs="+", default=["SOLUSDT", "XRPUSDT"])
    add_ch_args(p)
    p.add_argument("--cache", type=Path, default=Path.home() / ".cache" / "market-ingest" / "ob200")
    p.add_argument("--align", choices=("seq", "time"), default="seq")
    p.add_argument("--pick", choices=("ge", "le"), default="ge", help=argparse.SUPPRESS)
    p.add_argument("--lag-ms", type=int, default=30,
                   help="--align time: archive messages stamped up to T + this many ms "
                        "belong to second T (+30 test site, about -150 on production)")
    a = p.parse_args()
    url = ch_url(a.ch, a.ch_cmd)
    jobs = []
    for f in a.file or []:
        m = re.search(r"_([A-Z0-9]+)_ob200", f.name)
        jobs.append((m.group(1) if m else a.symbols[0], f.name, f))
    for day in a.date or []:
        for sym in a.symbols:
            src = ARCHIVE.format(s=sym, d=day)
            jobs.append((sym, day, download(src, a.cache / Path(src).name)))
    if not jobs:
        p.error("give --date or --file")
    for sym, label, path in jobs:
        if a.align == "seq":
            t0, t1 = feed_range(path)
            ours = load_ours(url, sym, t0, t1)
            arch, breaks = replay_by_seq(path, ours, a.pick)
        else:
            arch, breaks = replay(path, a.lag_ms)
            ours = load_ours(url, sym, min(arch), max(arch)) if arch else {}
        if not arch:
            print(f"\n== {sym} {label}: nothing to compare")
            continue
        report(sym, label, arch, ours, breaks)


if __name__ == "__main__":
    main()
