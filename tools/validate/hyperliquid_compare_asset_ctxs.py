"""Compare market.mark_price / market.open_interest (Hyperliquid) with Hyperliquid's asset_ctxs archive.

The archive (s3://hyperliquid-archive/asset_ctxs/YYYYMMDD.csv.lz4, requester-pays,
us-east-1) has one row per coin per minute: funding, open_interest (both sides,
in coins), oracle_px, mark_px, mid_px and more. The file for day D appears around
10 UTC on D+1 and may cover only part of the day: the time range it does cover
is printed, and only that range is read from ClickHouse. It lists the main DEX
only (no `dex:COIN` names), so HIP-3 symbols are not compared.

Each archive minute is matched to our row that is the first one in that minute
(argMin by event_time, i.e. closest to the minute start; we poll once a minute).
Ratios are ours / archive:
- oi: our open_interest column (half of the archive's) x 2;
- mark: mark_price; oracle: index_price; mid: extra.midPx;
- funding: share of minutes where |ours - archive| <= --funding-tol. It is a
  prediction that moves every few seconds and our poll comes ~10 s after the
  minute, so an exact match is not expected (hence the 1e-5 default).
Also shown: archive minutes without a row of ours (our gaps) and our minutes
without an archive row.

Needs the `aws` CLI with credentials (or --file with an already downloaded
.csv.lz4 / .csv) and the `lz4` package (pip install lz4) for .lz4 files.

    python tools/validate/hyperliquid_compare_asset_ctxs.py --date 2026-11-09 \\
        --ch 'http://172.16.0.106:8123/?user=ro'
"""

from __future__ import annotations

import argparse
import calendar
import csv
import io
import shutil
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path

from _common import HEADER, add_ch_args, add_window_args, ch_query, ch_url, fmt, summary, ts

BUCKET = "s3://hyperliquid-archive/asset_ctxs/{d}.csv.lz4"
FOCUS = ["BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "ADA", "LINK", "AVAX", "SUI",
         "LTC", "TRX", "DOT", "NEAR", "APT", "ARB", "OP", "HYPE", "FIL", "INJ"]
METRICS = ("oi", "mark", "oracle", "mid")


def fetch(day: str, cache: Path) -> Path:
    """Copy the day's archive file into the cache (an existing file is reused)."""
    dest = cache / f"{day.replace('-', '')}.csv.lz4"
    if dest.exists():
        return dest
    if not shutil.which("aws"):
        sys.exit("the `aws` CLI is not on PATH: install it, or download the file yourself "
                 "and pass it with --file")
    cache.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    r = subprocess.run(["aws", "s3", "cp", BUCKET.format(d=day.replace("-", "")), str(tmp),
                        "--request-payer", "requester", "--region", "us-east-1"])
    if r.returncode:
        tmp.unlink(missing_ok=True)
        sys.exit(f"aws s3 cp failed ({r.returncode})")
    tmp.replace(dest)
    return dest


def load_archive(path: Path) -> dict[str, dict[int, dict]]:
    """coin -> {minute (unix s) -> row}"""
    raw = path.read_bytes()
    if path.suffix == ".lz4":
        try:
            import lz4.frame
        except ImportError:
            sys.exit("the `lz4` package is needed for .lz4 files: pip install lz4 "
                     "(or pass a decompressed .csv to --file)")
        raw = lz4.frame.decompress(raw)
    out: dict[str, dict[int, dict]] = defaultdict(dict)
    for r in csv.DictReader(io.StringIO(raw.decode())):
        t = calendar.timegm(time.strptime(r["time"], "%Y-%m-%dT%H:%M:%SZ"))
        out[r["coin"]][t] = r
    return out


def load_ours(url: str, symbols: list[str] | None, t0: int, t1: int) -> dict[str, dict[int, dict]]:
    """symbol -> {minute -> our first row in that minute}; symbols None = the main DEX."""
    cond = (f"symbol IN ({', '.join(repr(s) for s in symbols)})" if symbols
            else "position(symbol, ':') = 0")
    where = (f"exchange = 'hyperliquid' AND {cond} "
             f"AND event_time >= {ts(t0)} AND event_time < {ts(t1)}")
    rows = ch_query(url, f"""
        SELECT symbol, toUnixTimestamp(toStartOfMinute(event_time)) AS m,
               argMin(mark_price, event_time) AS mark, argMin(index_price, event_time) AS oracle,
               argMin(funding_rate, event_time) AS funding,
               argMin(JSONExtractFloat(extra, 'midPx'), event_time) AS mid
        FROM market.mark_price WHERE {where} GROUP BY symbol, m""")
    oi = ch_query(url, f"""
        SELECT symbol, toUnixTimestamp(toStartOfMinute(event_time)) AS m,
               argMin(open_interest, event_time) AS oi
        FROM market.open_interest WHERE {where} GROUP BY symbol, m""")
    out: dict[str, dict[int, dict]] = defaultdict(dict)
    for r in rows:
        out[r["symbol"]][int(r["m"])] = r
    for r in oi:
        if int(r["m"]) in out[r["symbol"]]:
            out[r["symbol"]][int(r["m"])]["oi"] = r["oi"]
        else:
            out[r["symbol"]][int(r["m"])] = r
    return out


def compare(arch: dict[int, dict], ours: dict[int, dict], tol: float) -> dict:
    ratios: dict[str, list[float]] = {m: [] for m in METRICS}
    f_ok = f_bad = gaps = 0
    for t, a in arch.items():
        o = ours.get(t)
        if o is None:
            gaps += 1
            continue
        pairs = {"oi": (float(o["oi"]) * 2 if "oi" in o else 0, float(a["open_interest"])),
                 "mark": (float(o["mark"]) if "mark" in o else 0, float(a["mark_px"])),
                 "oracle": (float(o["oracle"]) if "oracle" in o else 0, float(a["oracle_px"])),
                 "mid": (float(o["mid"]) if "mid" in o else 0, float(a["mid_px"] or 0))}
        for m, (x, y) in pairs.items():
            if x > 0 and y > 0:
                ratios[m].append(x / y)
        if "funding" in o:
            if abs(float(o["funding"]) - float(a["funding"])) <= tol:
                f_ok += 1
            else:
                f_bad += 1
    return {"ratios": ratios, "f_ok": f_ok, "f_bad": f_bad, "gaps": gaps,
            "extra": sum(t not in arch for t in ours)}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    add_window_args(p)
    p.add_argument("--file", type=Path, help="an already downloaded .csv.lz4 or .csv (one day)")
    p.add_argument("--symbols", nargs="+", default=FOCUS, help="main DEX coins as stored")
    p.add_argument("--all", action="store_true", help="every coin, one summary line")
    add_ch_args(p)
    p.add_argument("--cache", type=Path,
                   default=Path.home() / ".cache" / "market-ingest" / "asset_ctxs")
    p.add_argument("--funding-tol", type=float, default=1e-5,
                   help="funding counts as matching within this absolute difference (default 1e-5)")
    p.add_argument("--max-off", type=float, default=0.001,
                   help="exit 1 if an oi or mark median is further from 1 than this (default 0.001)")
    p.add_argument("--min-funding", type=float, default=0.8,
                   help="exit 1 if the funding match share is below this (default 0.8)")
    a = p.parse_args()
    if not a.file and not a.date:
        sys.exit("pass --date (the file is downloaded with aws) or --file")
    url = ch_url(a.ch, a.ch_cmd)
    files = [a.file] if a.file else [fetch(d, a.cache) for d in a.date]
    bad = 0
    for path in files:
        arch = load_archive(path)
        times = [t for c in arch.values() for t in c]
        t0, t1 = min(times), max(times) + 60
        mins = (t1 - t0) // 60
        fmt_t = lambda t: time.strftime("%Y-%m-%d %H:%M", time.gmtime(t))  # noqa: E731
        print(f"\n== hyperliquid asset_ctxs {path.name}: {fmt_t(t0)} - {fmt_t(t1 - 60)} UTC, "
              f"{mins} minutes ({mins / 1440 * 100:.0f}% of a day), {len(arch)} coins")
        coins = sorted(arch) if a.all else [c for c in a.symbols if c in arch]
        for c in a.symbols if not a.all else []:
            if c not in arch:
                print(f"{c}: not in the archive")
        ours = load_ours(url, None if a.all else coins, t0, t1)
        res = {c: compare(arch[c], ours.get(c, {}), a.funding_tol) for c in coins}

        flagged = []
        missing = [c for c, r in res.items() if r["gaps"] == len(arch[c])]
        for c, r in res.items():
            if c in missing:
                continue
            fn = r["f_ok"] + r["f_bad"]
            off = [m for m in ("oi", "mark") if r["ratios"][m]
                   and abs(summary(r["ratios"][m])["median"] - 1) > a.max_off]
            no_data = not r["ratios"]["oi"] or not r["ratios"]["mark"]
            low_f = fn and r["f_ok"] / fn < a.min_funding
            if off or no_data or low_f:
                flagged.append(c)
        bad += bool(flagged)

        print("ours / archive (matched minutes)")
        print(f"{'coin':10} {'metric':6} {HEADER}")
        if a.all:
            for m in METRICS:
                pooled = [x for r in res.values() for x in r["ratios"][m]]
                print(f"{'all':10} {m:6} {fmt(summary(pooled))}")
        else:
            for c, r in res.items():
                for m in METRICS:
                    print(f"{c:10} {m:6} {fmt(summary(r['ratios'][m]))}")
        print(f"\n{'coin':10} {'funding ok':>10} {'differ':>7} {'our gaps':>9} {'no archive':>11}")
        if a.all:
            tot = {k: sum(r[k] for r in res.values()) for k in ("f_ok", "f_bad", "gaps", "extra")}
            fn = tot["f_ok"] + tot["f_bad"]
            share = f"{tot['f_ok'] / fn * 100:9.1f}%" if fn else f"{'-':>10}"
            print(f"{'all':10} {share} {tot['f_bad']:7} {tot['gaps']:9} {tot['extra']:11}")
        else:
            for c, r in res.items():
                fn = r["f_ok"] + r["f_bad"]
                share = f"{r['f_ok'] / fn * 100:9.1f}%" if fn else f"{'-':>10}"
                print(f"{c:10} {share} {r['f_bad']:7} {r['gaps']:9} {r['extra']:11}")
        if missing:
            print(f"no rows of ours at all ({len(missing)}; delisted or not collected): "
                  + ", ".join(missing))
        print("flagged: " + (", ".join(flagged) if flagged else "none")
              + f"  (oi/mark median off by > {a.max_off}, no data, or funding match < {a.min_funding:.0%})")
    print("\nour gaps: archive minutes without a row of ours; no archive: our minutes the "
          "archive lacks. HIP-3 (dex:COIN) is not in the archive.")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
