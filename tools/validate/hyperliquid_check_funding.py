"""Check Hyperliquid funding in market.mark_price against what Hyperliquid paid.

Hyperliquid's info API (POST /info {"type": "fundingHistory"}) lists, per coin
and for its whole history, every hourly payment: the rate paid and the hour's
average premium. Our rows are one poll a minute of metaAndAssetCtxs, where
`funding` is the rate predicted for the running hour and `premium` the current
premium. For every full hour:

- rate: our last `funding` before the hour ends against the rate paid at it.
  The last prediction sometimes misses the paid rate (small rates most), as
  on Binance and Bybit: a coin is flagged when fewer than half of its hours
  are within --rate-tol (25 %), which a unit or field error misses entirely;
- premium: the average of our minute premiums against the hour's average
  (Hyperliquid averages one sample every few seconds, we one a minute, so a
  small difference is expected);
- formula (main DEX only): the rate paid is
  (P + clamp(0.0001 - P, -0.0005, 0.0005)) / 8 with P the hour's premium.
  That holds only if `funding` is a rate per hour, the meaning the collector
  stores (funding_interval_h = 1). HIP-3 deployers scale funding by their own
  multiplier, so there the implied multiplier is shown instead.

    python tools/validate/hyperliquid_check_funding.py --hours 24 \\
        --ch 'http://172.16.0.106:8123/?user=ro'
    python tools/validate/hyperliquid_check_funding.py --date 2026-11-05 \\
        --symbols BTC MERL KAITO xyz:TWST --ch-cmd "sh ~/bin/prod-sql"
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
import urllib.request

from _common import add_ch_args, add_window_args, ch_query, ch_url, ts, windows

INFO = "https://api.hyperliquid.xyz/info"
FOCUS = ["BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "ADA", "LINK", "AVAX", "SUI",
         "LTC", "TRX", "DOT", "NEAR", "APT", "ARB", "OP", "HYPE", "FIL", "INJ"]
HOUR = 3600


def formula(p: float) -> float:
    """Hyperliquid's hourly rate from the hour's average premium."""
    return (p + min(max(0.0001 - p, -0.0005), 0.0005)) / 8


def history(coin: str, t0: int, t1: int) -> dict[int, tuple[float, float]]:
    """hour (unix s) -> (rate paid, premium)"""
    body = json.dumps({"type": "fundingHistory", "coin": coin,
                       "startTime": t0 * 1000, "endTime": t1 * 1000}).encode()
    req = urllib.request.Request(INFO, data=body, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        rows = json.load(r)
    out = {}
    for x in rows if isinstance(rows, list) else []:
        hour = round(int(x["time"]) / 1000 / HOUR) * HOUR      # paid a few ms after :00
        out[hour] = (float(x["fundingRate"]), float(x["premium"]))
    return out


def ours(url: str, symbols: list[str], t0: int, t1: int) -> dict[tuple[str, int], dict]:
    """(symbol, hour the payment is due) -> last funding, average premium, rows"""
    names = ", ".join(f"'{s}'" for s in symbols)
    rows = ch_query(url, f"""
        SELECT symbol, toUnixTimestamp(toStartOfHour(event_time)) + 3600 AS due,
               argMax(funding_rate, event_time) AS last_funding,
               avg(JSONExtractFloat(extra, 'premium')) AS avg_premium,
               count() AS n
        FROM market.mark_price
        WHERE exchange = 'hyperliquid' AND symbol IN ({names})
          AND event_time >= {ts(t0)} AND event_time < {ts(t1)}
        GROUP BY symbol, due""")
    return {(r["symbol"], int(r["due"])): r for r in rows}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    add_window_args(p)
    p.add_argument("--symbols", nargs="+", default=FOCUS,
                   help="Hyperliquid coins as stored (BTC, kPEPE, xyz:TSLA)")
    add_ch_args(p)
    p.add_argument("--rate-tol", type=float, default=0.25,
                   help="our last rate may differ from the paid one by this share (default 0.25)")
    a = p.parse_args()
    url = ch_url(a.ch, a.ch_cmd)
    bad = 0
    for label, t0, t1 in windows(a):
        mine = ours(url, a.symbols, t0, t1)
        print(f"\n== hyperliquid funding, {label}")
        print(f"{'coin':12} {'hours':>5} {'rate ok':>8} {'|d rate| med':>13} {'|d prem| med':>13} "
              f"{'formula':>9}")
        for coin in a.symbols:
            paid = history(coin, t0, t1 + 60)
            time.sleep(0.2)                      # 20 weight a call of the IP's 1200 a minute
            full = max((int(r["n"]) for (c, _), r in mine.items() if c == coin), default=0)
            d_rate, d_prem, ok, f_ok, f_n, mult = [], [], 0, 0, 0, []
            for hour, (rate, prem) in sorted(paid.items()):
                r = mine.get((coin, hour))
                if not r or int(r["n"]) < 0.8 * full:    # a partial hour: the average would lie
                    continue
                last, avg_p = float(r["last_funding"]), float(r["avg_premium"])
                d_rate.append(abs(last - rate))
                d_prem.append(abs(avg_p - prem))
                ok += abs(last - rate) <= max(a.rate_tol * abs(rate), 1e-8)
                calc = formula(prem)
                if ":" in coin:
                    if calc:
                        mult.append(rate / calc)
                else:
                    f_n += 1
                    f_ok += abs(rate - calc) <= 1e-9 + 1e-6 * abs(rate)
            n = len(d_rate)
            if not n:
                print(f"{coin:12} {0:5}   no full hour with both our rows and a payment")
                continue
            share = ok / n
            fcell = (f"{f_ok}/{f_n}" if f_n else
                     f"x{statistics.median(mult):.3f}" if mult else "-")
            flag = "" if share >= 0.5 and f_ok == f_n else "  <-- check"
            bad += bool(flag)
            print(f"{coin:12} {n:5} {share * 100:7.0f}% {statistics.median(d_rate) * 1e6:10.3f}e-6 "
                  f"{statistics.median(d_prem) * 1e4:10.3f} bp {fcell:>9}{flag}")
    print("\nrate ok: hours where our last funding is within --rate-tol of the paid rate "
          "(flagged under 50 %). formula: hours where the "
          "paid rate equals (P + clamp(0.0001 - P, +-0.0005)) / 8; HIP-3: median paid / formula.")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
