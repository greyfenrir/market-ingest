"""Check Binance and Bybit open interest against the venues' own OI history.

market.open_interest must hold ONE side, in the base coin, for every venue.
Both venues keep a 5-minute OI history:
- Binance GET /futures/data/openInterestHist (sumOpenInterest, 30 days);
- Bybit GET /v5/market/open-interest (openInterest, intervalTime=5min). Like
  the ticker's openInterest it counts both sides (checked 2026-11-06: ours is
  exactly 0.500 of it on all 20 focus symbols), so it is halved here.
Our row nearest to each 5-minute mark (within --max-dist s) is compared with
it. The median ratio ours/history should be 1.000; 0.5 or 2 means sides are
counted differently, a large factor means units (contracts, USD). OI moves
little in seconds, so the spread is small.

    python tools/validate/binance_bybit_check_oi.py --hours 24 \\
        --ch 'http://172.16.0.106:8123/?user=ro'
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.parse
import urllib.request

from _common import add_ch_args, add_window_args, ch_query, ch_url, summary, ts, windows

FOCUS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT", "DOGEUSDT", "ADAUSDT",
         "LINKUSDT", "AVAXUSDT", "SUIUSDT", "LTCUSDT", "TRXUSDT", "DOTUSDT", "NEARUSDT",
         "APTUSDT", "ARBUSDT", "OPUSDT", "HYPEUSDT", "FILUSDT", "INJUSDT"]
VENUES = ("binance_um", "bybit_linear")
STEP = 300
SIDES = {"binance_um": 1, "bybit_linear": 2}   # sides counted in the history


def get(url: str, params: dict):
    req = urllib.request.Request(url + "?" + urllib.parse.urlencode(params),
                                 headers={"User-Agent": "market-ingest-validate"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def history(venue: str, sym: str, t0: int, t1: int) -> dict[int, float]:
    """5-minute mark (unix s) -> OI, paged by 200 points"""
    out: dict[int, float] = {}
    start = t0
    while start < t1:
        end = min(t1, start + 200 * STEP)
        if venue == "binance_um":
            rows = get("https://fapi.binance.com/futures/data/openInterestHist",
                       {"symbol": sym, "period": "5m", "startTime": start * 1000,
                        "endTime": end * 1000, "limit": 500})
            pairs = [(x["timestamp"], x["sumOpenInterest"]) for x in rows]
        else:
            d = get("https://api.bybit.com/v5/market/open-interest",
                    {"category": "linear", "symbol": sym, "intervalTime": "5min",
                     "startTime": start * 1000, "endTime": end * 1000, "limit": 200})
            pairs = [(x["timestamp"], x["openInterest"])
                     for x in d.get("result", {}).get("list", [])]
        for t, v in pairs:
            out[int(t) // 1000] = float(v) / SIDES[venue]
        start = end
        time.sleep(0.1)
    return out


def ours(url: str, venue: str, symbols: list[str], t0: int, t1: int, max_dist: int) -> dict:
    """(symbol, 5-minute mark) -> our OI nearest to it"""
    names = ", ".join(f"'{s}'" for s in symbols)
    rows = ch_query(url, f"""
        SELECT symbol, mark, argMin(open_interest, dist) AS oi, min(dist) AS d
        FROM (SELECT symbol, open_interest,
                     intDiv(toUnixTimestamp(event_time) + {STEP // 2}, {STEP}) * {STEP} AS mark,
                     abs(toUnixTimestamp64Milli(event_time) / 1000 - mark) AS dist
              FROM market.open_interest
              WHERE exchange = '{venue}' AND symbol IN ({names})
                AND event_time >= {ts(t0 - STEP)} AND event_time < {ts(t1 + STEP)})
        GROUP BY symbol, mark HAVING d <= {max_dist}""")
    return {(r["symbol"], int(r["mark"])): float(r["oi"]) for r in rows}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    add_window_args(p)
    p.add_argument("--symbols", nargs="+", default=FOCUS)
    p.add_argument("--venues", nargs="+", default=list(VENUES), choices=VENUES)
    add_ch_args(p)
    p.add_argument("--max-dist", type=int, default=30,
                   help="our row may be this many seconds from the 5-minute mark (default 30)")
    p.add_argument("--tol", type=float, default=0.02,
                   help="flag a symbol whose median ratio is further than this from 1")
    a = p.parse_args()
    url = ch_url(a.ch, a.ch_cmd)
    bad = 0
    for label, t0, t1 in windows(a):
        for venue in a.venues:
            mine = ours(url, venue, a.symbols, t0, t1, a.max_dist)
            print(f"\n== {venue} open interest vs its 5-min history, {label}")
            print(f"{'symbol':10} {'median':>7} {'p5-p95':>13} {'<=1%':>5} {'<=5%':>5}")
            for sym in a.symbols:
                ref = history(venue, sym, t0, t1)
                ratios = [mine[(sym, t)] / v for t, v in ref.items()
                          if (sym, t) in mine and v > 0]
                st = summary(ratios)
                if not st["n"]:
                    print(f"{sym:10} no 5-minute mark with both a history point and our row")
                    bad += 1
                    continue
                flag = abs(st["median"] - 1) > a.tol
                bad += flag
                print(f"{sym:10} {st['median']:7.3f} {st['p5']:6.3f}-{st['p95']:<6.3f} "
                      f"{st['in1'] * 100:4.0f}% {st['in5'] * 100:4.0f}% n={st['n']}"
                      f"{'  <-- check' if flag else ''}")
    print("\nratio = our OI / the venue's OI history at the same 5-minute mark. "
          "1.000 = one side in coins, as stored for every venue.")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
