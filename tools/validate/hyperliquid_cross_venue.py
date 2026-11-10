"""Check Hyperliquid prices against Binance and Bybit in our own tables.

Hyperliquid's oracle (oraclePx, stored as index_price) is a weighted median of
spot prices on other venues - the same thing Binance and Bybit publish as their
index. So, at the same second:

- oracle vs the venue's index: should agree to a few bp. A large or constant
  offset means a wrong field, a wrong coin, or a wrong scale (kPEPE is 1000
  PEPE, Binance's 1000PEPEUSDT);
- mark vs the venue's mark: close, but each venue has its own basis;
- funding, scaled to 8 h: Hyperliquid's hourly rate x 8 against the venue's
  rate x 8 / its interval. Not equal - funding differs between venues - but of
  the same order; a factor of 8 points at a unit error.

Our Hyperliquid row carries the second it was received; Binance and Bybit
write their focus symbols once a second, so the rows meet on the same second.
Other symbols are written once a minute (sampled = 1) and meet on the minute:
coarser, still plenty to catch a wrong scale (kPEPE vs 1000PEPEUSDT).

The thresholds are for errors, not for venue differences: the oracle and the
indexes are built from different sets of spot venues, which on a coin traded
mostly on Hyperliquid itself (HYPE) is ~10 bp. A wrong field or scale is
hundreds or thousands.
Binance does not store its funding interval yet (0): --assume-interval is used
for it (8; HYPEUSDT on Binance pays every 4 h, so its 8 h figure reads 2x).

    python tools/validate/hyperliquid_cross_venue.py --hours 24 \\
        --ch 'http://172.16.0.106:8123/?user=ro'
"""

from __future__ import annotations

import argparse
import sys

from _common import add_ch_args, add_window_args, ch_query, ch_url, ts, windows

FOCUS = ["BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "ADA", "LINK", "AVAX", "SUI",
         "LTC", "TRX", "DOT", "NEAR", "APT", "ARB", "OP", "HYPE", "FIL", "INJ", "kPEPE"]
VENUES = ("binance_um", "bybit_linear")


def venue_symbol(coin: str, overrides: dict[str, str]) -> str:
    """BTC -> BTCUSDT, kPEPE -> 1000PEPEUSDT."""
    if coin in overrides:
        return overrides[coin]
    if coin.startswith("k") and coin[1:].isupper():
        return f"1000{coin[1:]}USDT"
    return f"{coin}USDT"


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    add_window_args(p)
    p.add_argument("--symbols", nargs="+", default=FOCUS, help="Hyperliquid coins (main DEX)")
    p.add_argument("--map", nargs="*", default=[], metavar="COIN=SYMBOL",
                   help="venue symbol where the rule (COIN+USDT, kX -> 1000XUSDT) does not fit")
    add_ch_args(p)
    p.add_argument("--assume-interval", type=int, default=8,
                   help="funding interval in hours for rows that do not carry one (Binance)")
    p.add_argument("--max-oracle-bp", type=float, default=25,
                   help="flag a coin whose median oracle - index differs by more (default 25)")
    p.add_argument("--max-oracle-p99-bp", type=float, default=100)
    a = p.parse_args()
    url = ch_url(a.ch, a.ch_cmd)
    overrides = dict(x.split("=", 1) for x in a.map)
    mapping = {venue_symbol(c, overrides): c for c in a.symbols}
    keys = ", ".join(f"'{v}'" for v in mapping)
    coins = ", ".join(f"'{c}'" for c in mapping.values())
    to_coin = (f"transform(symbol, [{keys}], [{coins}], '')")
    bad = 0
    for label, t0, t1 in windows(a):
        rows = ch_query(url, f"""
            SELECT h.symbol AS coin, v.exchange AS venue, count() AS n, any(v.per) AS per,
                   quantile(0.5)((h.index_price / v.vi - 1) * 1e4) AS oracle_p50,
                   quantile(0.99)(abs(h.index_price / v.vi - 1) * 1e4) AS oracle_p99,
                   quantile(0.5)((h.mark_price / v.vm - 1) * 1e4) AS mark_p50,
                   quantile(0.99)(abs(h.mark_price / v.vm - 1) * 1e4) AS mark_p99,
                   quantile(0.5)(h.funding_rate * 8) AS hl_f8,
                   quantile(0.5)(v.vf * 8 / if(v.fi > 0, v.fi, {a.assume_interval})) AS v_f8,
                   max(v.fi) AS v_interval
            FROM (SELECT symbol, mark_price, index_price, funding_rate,
                         arrayJoin([(toStartOfSecond(event_time), 'second'),
                                    (toStartOfMinute(event_time), 'minute')]) AS k,
                         k.1 AS s, k.2 AS per
                  FROM market.mark_price
                  WHERE exchange = 'hyperliquid' AND symbol IN ({coins})
                    AND event_time >= {ts(t0)} AND event_time < {ts(t1)}) AS h
            INNER JOIN
                 (SELECT exchange, {to_coin} AS coin,
                         if(sampled = 0, toStartOfSecond(event_time), toStartOfMinute(event_time)) AS s,
                         if(sampled = 0, 'second', 'minute') AS per,
                         any(mark_price) AS vm, any(index_price) AS vi,
                         any(funding_rate) AS vf, any(funding_interval_h) AS fi
                  FROM market.mark_price
                  WHERE exchange IN ({", ".join(f"'{x}'" for x in VENUES)})
                    AND symbol IN ({keys})
                    AND event_time >= {ts(t0)} AND event_time < {ts(t1)}
                    AND index_price > 0 AND mark_price > 0
                  GROUP BY exchange, coin, s, per) AS v
            ON h.symbol = v.coin AND h.s = v.s AND h.per = v.per
            GROUP BY coin, venue
            ORDER BY coin, venue""")
        print(f"\n== hyperliquid vs other venues, {label}")
        print(f"{'coin':8} {'venue':13} {'n':>5} {'per':6} | {'oracle-index bp':>15} {'p99':>6} | "
              f"{'mark-mark bp':>12} {'p99':>6} | {'funding x8: hl':>14} {'venue':>10}")
        seen = set()
        for r in rows:
            seen.add(r["coin"])
            o50, o99 = float(r["oracle_p50"]), float(r["oracle_p99"])
            flag = abs(o50) > a.max_oracle_bp or o99 > a.max_oracle_p99_bp
            bad += flag
            interval = int(r["v_interval"]) or f"{a.assume_interval}?"
            print(f"{r['coin']:8} {r['venue']:13} {int(r['n']):5} {r['per']:6} | {o50:15.2f} {o99:6.1f} | "
                  f"{float(r['mark_p50']):12.2f} {float(r['mark_p99']):6.1f} | "
                  f"{float(r['hl_f8']) * 1e4:11.2f} bp {float(r['v_f8']) * 1e4:7.2f} bp"
                  f" (/{interval}h){'  <-- check' if flag else ''}")
        for c in a.symbols:
            if c not in seen:
                print(f"{c:8} no matching seconds (venue symbol {venue_symbol(c, overrides)}?)")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
