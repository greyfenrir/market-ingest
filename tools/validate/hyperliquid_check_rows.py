"""Check Hyperliquid rows for internal consistency and the polls for gaps.

No outside reference needed: every row must obey Hyperliquid's own rules, and
the polls must arrive on their schedule.

Rows (per DEX: main and each HIP-3 DEX), each a hard failure when non-zero:
- midPx lies outside impactPxs (impact bid <= mid <= impact ask);
- |funding| above 4 % an hour, Hyperliquid's cap;
- a price <= 0, or open interest < 0;
- open interest that is not half of the venue's figure kept in extra
  (Hyperliquid reports both sides, the column stores one);
- next_funding_time is not the next full hour; funding_interval_h is not 1;
  sampled is not 1;
- the same (symbol, event_time) twice; a mark row without its open interest
  row or the other way round.

Shown, not failed: mark more than --max-mark-off % from the oracle (thin HIP-3
markets can be), rows without impactPxs, zero open interest, the longest
run of identical marks of the focus coins (a frozen answer), and open interest
that changes more than --max-oi-jump times between two polls of a coin.

Polls: the main DEX once a minute - minutes without a poll are listed, and
polls with fewer coins than usual; HIP-3 once per round (default 5 min) -
rounds per hour and DEXes per round.

    python tools/validate/hyperliquid_check_rows.py --hours 24 \\
        --ch 'http://172.16.0.106:8123/?user=ro'
"""

from __future__ import annotations

import argparse
import statistics
import sys
import time

from _common import add_ch_args, add_window_args, ch_query, ch_url, ts, windows

FOCUS = ["BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "ADA", "LINK", "AVAX", "SUI",
         "LTC", "TRX", "DOT", "NEAR", "APT", "ARB", "OP", "HYPE", "FIL", "INJ"]
HARD = ("mid_outside_impact", "funding_over_cap", "price_le0", "oi_neg", "oi_not_half", "next_funding_bad",
        "interval_bad", "sampled_bad", "dup_rows")


def has_column(url: str, table: str, column: str) -> bool:
    return bool(ch_query(url, f"""SELECT 1 AS x FROM system.columns WHERE database = 'market'
                                  AND table = '{table}' AND name = '{column}'"""))


def rows_check(url: str, t0: int, t1: int, max_off: float, with_interval: bool) -> list[dict]:
    interval = "countIf(m.funding_interval_h != 1)" if with_interval else "toUInt64(0)"
    return ch_query(url, f"""
        SELECT if(position(m.symbol, ':') = 0, 'main', splitByChar(':', m.symbol)[1]) AS dex,
               count() AS rows, uniqExact(m.symbol) AS coins,
               countIf(length(ip) = 2 AND mid > 0
                       AND (mid < toFloat64OrZero(ip[1]) OR mid > toFloat64OrZero(ip[2])))
                   AS mid_outside_impact,
               countIf(length(ip) != 2) AS no_impact,
               countIf(abs(m.funding_rate) > 0.04) AS funding_over_cap,
               countIf(m.mark_price <= 0 OR m.index_price <= 0) AS price_le0,
               countIf(o.open_interest < 0) AS oi_neg,
               countIf(o.open_interest = 0) AS oi_zero,
               countIf(o.raw > 0 AND abs(o.open_interest * 2 / o.raw - 1) > 1e-9) AS oi_not_half,
               countIf(m.next_funding_time != toStartOfHour(m.event_time) + INTERVAL 1 HOUR)
                   AS next_funding_bad,
               {interval} AS interval_bad,
               countIf(m.sampled != 1) AS sampled_bad,
               countIf(abs(m.mark_price / m.index_price - 1) * 100 > {max_off}) AS mark_off_oracle
        FROM (SELECT *, JSONExtractFloat(extra, 'midPx') AS mid,
                     JSONExtract(extra, 'impactPxs', 'Array(String)') AS ip
              FROM market.mark_price
              WHERE exchange = 'hyperliquid'
                AND event_time >= {ts(t0)} AND event_time < {ts(t1)}) AS m
        LEFT JOIN (SELECT symbol, event_time, open_interest,
                          toFloat64OrZero(JSONExtractString(extra, 'openInterest')) AS raw
                   FROM market.open_interest
                   WHERE exchange = 'hyperliquid'
                     AND event_time >= {ts(t0)} AND event_time < {ts(t1)}) AS o
        ON o.symbol = m.symbol AND o.event_time = m.event_time
        GROUP BY dex ORDER BY dex = 'main' DESC, dex""")


def pairing(url: str, t0: int, t1: int) -> dict:
    w = f"exchange = 'hyperliquid' AND event_time >= {ts(t0)} AND event_time < {ts(t1)}"
    r = ch_query(url, f"""
        SELECT (SELECT count() - uniqExact(symbol, event_time) FROM market.mark_price WHERE {w})
                   AS dup_mark,
               (SELECT count() - uniqExact(symbol, event_time) FROM market.open_interest WHERE {w})
                   AS dup_oi,
               (SELECT count() FROM (SELECT symbol, event_time FROM market.mark_price WHERE {w}
                                     EXCEPT SELECT symbol, event_time FROM market.open_interest
                                     WHERE {w})) AS mark_without_oi,
               (SELECT count() FROM (SELECT symbol, event_time FROM market.open_interest WHERE {w}
                                     EXCEPT SELECT symbol, event_time FROM market.mark_price
                                     WHERE {w})) AS oi_without_mark""")
    return r[0]


def oi_jumps(url: str, t0: int, t1: int, factor: float) -> list[dict]:
    """Coins whose open interest moved more than `factor` times between polls."""
    return ch_query(url, f"""
        SELECT symbol, count() AS jumps, max(greatest(r, 1 / r)) AS worst FROM (
            SELECT symbol, open_interest / lagInFrame(open_interest) OVER
                       (PARTITION BY symbol ORDER BY event_time
                        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS r
            FROM market.open_interest
            WHERE exchange = 'hyperliquid'
              AND event_time >= {ts(t0)} AND event_time < {ts(t1)})
        WHERE r > 0 AND isFinite(r) AND greatest(r, 1 / r) > {factor}
        GROUP BY symbol ORDER BY worst DESC LIMIT 10""")


def frozen(url: str, t0: int, t1: int, focus: list[str]) -> list[dict]:
    names = ", ".join(f"'{s}'" for s in focus)
    return ch_query(url, f"""
        SELECT symbol, max(run) AS longest_run FROM (
            SELECT symbol, count() AS run FROM (
                SELECT symbol, sum(changed) OVER (PARTITION BY symbol ORDER BY event_time
                                                  ROWS UNBOUNDED PRECEDING) AS grp
                FROM (SELECT symbol, event_time,
                             mark_price != lagInFrame(mark_price, 1, -1.0) OVER
                                 (PARTITION BY symbol ORDER BY event_time
                                  ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS changed
                      FROM market.mark_price
                      WHERE exchange = 'hyperliquid' AND symbol IN ({names})
                        AND event_time >= {ts(t0)} AND event_time < {ts(t1)}))
            GROUP BY symbol, grp)
        GROUP BY symbol ORDER BY longest_run DESC LIMIT 5""")


def polls(url: str, t0: int, t1: int) -> tuple[list[dict], list[dict]]:
    main = ch_query(url, f"""
        SELECT toUnixTimestamp(toStartOfMinute(event_time)) AS m, count() AS rows,
               uniqExact(event_time) AS polls
        FROM market.mark_price
        WHERE exchange = 'hyperliquid' AND position(symbol, ':') = 0
          AND event_time >= {ts(t0)} AND event_time < {ts(t1)}
        GROUP BY m ORDER BY m""")
    hip3 = ch_query(url, f"""
        SELECT toUnixTimestamp(toStartOfHour(event_time)) AS h,
               uniqExact(toStartOfFiveMinutes(event_time)) AS rounds,
               uniqExact(splitByChar(':', symbol)[1]) AS dexes, count() AS rows
        FROM market.mark_price
        WHERE exchange = 'hyperliquid' AND position(symbol, ':') > 0
          AND event_time >= {ts(t0)} AND event_time < {ts(t1)}
        GROUP BY h ORDER BY h""")
    return main, hip3


def hm(t: int) -> str:
    return time.strftime("%m-%d %H:%M", time.gmtime(t))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    add_window_args(p)
    add_ch_args(p)
    p.add_argument("--max-mark-off", type=float, default=5,
                   help="show rows whose mark is further than this %% from the oracle")
    p.add_argument("--max-oi-jump", type=float, default=2,
                   help="show coins whose open interest moved more than this many times "
                        "between two polls")
    a = p.parse_args()
    url = ch_url(a.ch, a.ch_cmd)
    with_interval = has_column(url, "mark_price", "funding_interval_h")
    failed = False
    for label, t0, t1 in windows(a):
        print(f"\n== hyperliquid rows, {label}")
        per_dex = rows_check(url, t0, t1, a.max_mark_off, with_interval)
        if not per_dex:
            print("no Hyperliquid rows in this window")
            continue
        cols = ["rows", "coins", *HARD[:-1], "no_impact", "oi_zero", "mark_off_oracle"]
        short = {"mid_outside_impact": "mid_out", "funding_over_cap": "f_cap",
                 "next_funding_bad": "nft_bad", "interval_bad": "int_bad",
                 "sampled_bad": "smp_bad", "mark_off_oracle": "mark_off",
                 "oi_not_half": "oi_half"}
        print(f"{'dex':6} " + " ".join(f"{short.get(c, c):>9}" for c in cols))
        for r in per_dex:
            print(f"{r['dex']:6} " + " ".join(f"{r[c]:>9}" for c in cols))
            failed |= any(int(r[c]) for c in HARD[:-1])
        if not with_interval:
            print("  (market.mark_price has no funding_interval_h: schema/006 not applied)")
        pr = pairing(url, t0, t1)
        print("pairing: " + ", ".join(f"{k} {v}" for k, v in pr.items()))
        failed |= any(int(v) for v in pr.values())
        jumps = oi_jumps(url, t0, t1, a.max_oi_jump)
        print(f"open interest moving more than x{a.max_oi_jump:g} between polls: "
              + (", ".join(f"{r['symbol']} {r['jumps']} (x{float(r['worst']):.1f})" for r in jumps)
                 or "none"))
        fr = frozen(url, t0, t1, FOCUS)
        print("longest run of identical marks (polls): "
              + ", ".join(f"{r['symbol']} {r['longest_run']}" for r in fr))

        main_polls, hip3 = polls(url, t0, t1)
        have = {int(r["m"]): r for r in main_polls}
        first = min(have) if have else t0
        missing = [m for m in range(first, t1, 60) if m not in have]
        sizes = [int(r["rows"]) // max(1, int(r["polls"])) for r in main_polls]
        usual = statistics.median(sizes) if sizes else 0
        short_polls = [(m, r) for m, r in have.items()
                       if int(r["rows"]) // max(1, int(r["polls"])) < 0.95 * usual]
        print(f"main DEX: {len(have)} minutes with a poll since {hm(first)}, "
              f"{len(missing)} without; usually {usual:g} coins a poll, "
              f"{len(short_polls)} polls with fewer")
        for m in missing[:10]:
            print(f"  no poll in {hm(m)}")
        for m, r in sorted(short_polls)[:5]:
            print(f"  {hm(m)}: {r['rows']} rows")
        if hip3:
            rounds = [int(r["rounds"]) for r in hip3]
            dexes = [int(r["dexes"]) for r in hip3]
            print(f"HIP-3: {len(hip3)} hours, rounds an hour min {min(rounds)} median "
                  f"{statistics.median(rounds):g} max {max(rounds)}; DEXes an hour "
                  f"min {min(dexes)} max {max(dexes)}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
