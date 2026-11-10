"""Check Binance and Bybit funding in market.mark_price against what was paid.

Both venues publish every funding payment: Binance GET /fapi/v1/fundingRate,
Bybit GET /v5/market/funding/history. Our rows carry the predicted rate and
next_funding_time. For every payment in the window:

- rate: our last funding_rate with next_funding_time = the payment, against
  the rate paid. Mostly equal, but on both venues the last prediction
  sometimes misses the settled rate by 5-50 % (small rates move most), even
  1 s before. A coin is flagged when fewer than half of its payments are
  within --rate-tol (25 %): a unit, sign or field error misses them all;
- age: how long before the payment our last such row is (a gap there leaves
  an old prediction);
- schedule: payment times we never announced in next_funding_time, and
  announced times nobody paid (a wrong interval or a shifted clock).

    python tools/validate/binance_bybit_check_funding.py --hours 48 \\
        --ch 'http://172.16.0.106:8123/?user=ro'
    python tools/validate/binance_bybit_check_funding.py --date 2026-11-05 \\
        --ch-cmd "sh ~/bin/prod-sql"
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
import urllib.parse
import urllib.request

from _common import add_ch_args, add_window_args, ch_query, ch_url, ts, windows

FOCUS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT", "DOGEUSDT", "ADAUSDT",
         "LINKUSDT", "AVAXUSDT", "SUIUSDT", "LTCUSDT", "TRXUSDT", "DOTUSDT", "NEARUSDT",
         "APTUSDT", "ARBUSDT", "OPUSDT", "HYPEUSDT", "FILUSDT", "INJUSDT"]
VENUES = ("binance_um", "bybit_linear")


def get(url: str, params: dict):
    req = urllib.request.Request(url + "?" + urllib.parse.urlencode(params),
                                 headers={"User-Agent": "market-ingest-validate"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def paid(venue: str, sym: str, t0: int, t1: int) -> dict[int, float]:
    """payment time (unix s, rounded to the minute) -> rate paid"""
    if venue == "binance_um":
        rows = get("https://fapi.binance.com/fapi/v1/fundingRate",
                   {"symbol": sym, "startTime": t0 * 1000, "endTime": t1 * 1000, "limit": 1000})
        pairs = [(x["fundingTime"], x["fundingRate"]) for x in rows]
    else:
        d = get("https://api.bybit.com/v5/market/funding/history",
                {"category": "linear", "symbol": sym, "startTime": t0 * 1000,
                 "endTime": t1 * 1000, "limit": 200})
        pairs = [(x["fundingRateTimestamp"], x["fundingRate"])
                 for x in d.get("result", {}).get("list", [])]
    out = {round(int(t) / 60000) * 60: float(r) for t, r in pairs}
    return {t: r for t, r in out.items() if t0 <= t < t1}      # endTime is inclusive


def ours(url: str, venue: str, symbols: list[str], t0: int, t1: int) -> dict[tuple[str, int], dict]:
    """(symbol, announced payment) -> last rate and the gap before the payment"""
    names = ", ".join(f"'{s}'" for s in symbols)
    # Bybit keeps announcing a payment for a few seconds after it, with the next rate,
    # hence event_time < next_funding_time. No SQL comments: --ch-cmd joins lines.
    rows = ch_query(url, f"""
        SELECT symbol, intDiv(toUnixTimestamp(next_funding_time) + 30, 60) * 60 AS due,
               argMax(funding_rate, event_time) AS last_rate,
               toUnixTimestamp(next_funding_time) - toUnixTimestamp(max(event_time)) AS gap_s,
               count() AS n
        FROM market.mark_price
        WHERE exchange = '{venue}' AND symbol IN ({names})
          AND event_time >= {ts(t0)} AND event_time < {ts(t1)}
          AND toUnixTimestamp(next_funding_time) > 0 AND event_time < next_funding_time
        GROUP BY symbol, next_funding_time""")
    out: dict[tuple[str, int], dict] = {}
    for r in rows:
        k = (r["symbol"], int(r["due"]))
        if k not in out or int(r["gap_s"]) < int(out[k]["gap_s"]):
            out[k] = r
    return out


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    add_window_args(p)
    p.add_argument("--symbols", nargs="+", default=FOCUS)
    p.add_argument("--venues", nargs="+", default=list(VENUES), choices=VENUES)
    add_ch_args(p)
    p.add_argument("--rate-tol", type=float, default=0.25,
                   help="our last rate may differ from the paid one by this share (default 0.25)")
    p.add_argument("--max-gap", type=int, default=300,
                   help="a last row older than this many seconds before the payment is not compared")
    a = p.parse_args()
    url = ch_url(a.ch, a.ch_cmd)
    bad = 0
    for label, t0, t1 in windows(a):
        for venue in a.venues:
            tol = a.rate_tol
            mine = ours(url, venue, a.symbols, t0, t1)
            print(f"\n== {venue} funding, {label}")
            print(f"{'symbol':10} {'paid':>4} {'cmp':>4} {'rate ok':>8} {'|d| med':>10} "
                  f"{'gap med':>8} {'unannounced':>11} {'unpaid':>6}")
            for sym in a.symbols:
                got = paid(venue, sym, t0, t1)
                time.sleep(0.1)
                # payments after our first row and before the window end
                first = min((d for (s, d) in mine if s == sym), default=None)
                due = {d: r for (s, d), r in mine.items() if s == sym}
                diffs, ok, gaps, miss = [], 0, [], []
                for t, rate in sorted(got.items()):
                    r = due.get(t)
                    if not r:
                        continue
                    gaps.append(int(r["gap_s"]))
                    if int(r["gap_s"]) > a.max_gap:
                        continue
                    d = abs(float(r["last_rate"]) - rate)
                    diffs.append(d)
                    good = d <= max(tol * abs(rate), 1e-7)
                    ok += good
                    if not good:
                        miss.append((t, float(r["last_rate"]), rate, int(r["gap_s"])))
                unann = [t for t in got if first is not None and t >= first and t not in due]
                unpaid = [t for t in due if t0 <= t < t1 and t not in got]
                n = len(diffs)
                share = ok / n if n else 0
                flag = ("" if n and share >= 0.5 and not unann and not unpaid
                        else "  <-- check")
                bad += bool(flag)
                print(f"{sym:10} {len(got):4} {n:4} {share * 100:7.0f}% "
                      f"{(statistics.median(diffs) * 1e6 if diffs else 0):7.2f}e-6 "
                      f"{(statistics.median(gaps) if gaps else 0):7.0f}s {len(unann):11} "
                      f"{len(unpaid):6}{flag}")
                for t, o, r, g in miss[:3]:
                    print(f"    {time.strftime('%m-%d %H:%M', time.gmtime(t))}: ours {o:.6e} "
                          f"paid {r:.6e}, last row {g}s before")
                for t in sorted(unann)[:3] + sorted(unpaid)[:3]:
                    kind = "unannounced" if t in unann else "unpaid"
                    print(f"    {kind}: {time.strftime('%m-%d %H:%M', time.gmtime(t))}")
    print("\nrate ok: payments where our last rate is within the tolerance of the paid one; a coin is "
          "flagged when it is under 50 %. "
          "gap: payment time - our last row announcing it. unannounced: paid, but no row of ours "
          "named that time; unpaid: we named a time in the window nobody paid.")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
