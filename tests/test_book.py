import math, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ingest.book import LocalBook, band_label, empty_aggregate

fails = []
def check(name, got, want):
    if got == want: print(f"  PASS  {name}")
    else: print(f"  FAIL  {name}: got {got!r}, want {want!r}"); fails.append(name)
def close(name, got, want, tol=1e-9):
    ok = got is not None and abs(got - want) <= tol * max(1.0, abs(want))
    if ok: print(f"  PASS  {name}")
    else: print(f"  FAIL  {name}: got {got!r}, want ~{want!r}"); fails.append(name)


# Mid 100. Each level is 1 unit, at these distances from mid:
#   bids 0.1, 0.4, 1.0, 1.5, 3.0      asks 0.1, 0.4, 1.0, 1.5, 3.0
BIDS = [["99.9", "1"], ["99.6", "1"], ["99.0", "1"], ["98.5", "1"], ["97.0", "1"]]
ASKS = [["100.1", "1"], ["100.4", "1"], ["101.0", "1"], ["101.5", "1"], ["103.0", "1"]]

def book(bids=BIDS, asks=ASKS, limit=None, uid=10):
    b = LocalBook()
    b.load_snapshot(bids, asks, uid, depth_limit=limit)
    return b


print("== band_label: column-safe band names")
check("0.2",  band_label(0.2), "0p2")
check("0.5",  band_label(0.5), "0p5")
check("1.0",  band_label(1.0), "1")
check("2",    band_label(2),   "2")

print("== snapshot and deltas")
b = book()
check("synced after snapshot",   b.synced, True)
check("update id from snapshot", b.last_update_id, 10)
check("best bid",                b.best_bid(), 99.9)
check("best ask",                b.best_ask(), 100.1)
b.apply([["99.9", "2.5"]], [], 11)
check("absolute qty replaces",   b.bids[99.9], 2.5)
b.apply([["99.95", "1"]], [], 12)
check("new level added",         b.best_bid(), 99.95)
b.apply([["99.95", "0.000"]], [], 13)
check("zero removes level",      99.95 in b.bids, False)
b.apply([["50", "0"]], [], 14)
check("removing unknown level is harmless", len(b.bids), 5)
check("update id advances",      b.last_update_id, 14)
b.apply([["99.90", "3"]], [], 15)
check("'99.90' and '99.9' are one level", (len(b.bids), b.bids[99.9]), (5, 3.0))
b2 = book(bids=[["99", "1"], ["98", "0"]], asks=[["101", "1"]])
check("zero-qty level in snapshot is skipped", sorted(b2.bids), [99.0])

print("== aggregate: best prices, spread, cumulative depth")
row = book(limit=5).aggregate()
close("mid",          row["mid"], 100.0)
close("spread bps",   row["spread_bps"], 20.0)
check("levels",       (row["levels_bid"], row["levels_ask"]), (5, 5))
check("best qty",     (row["best_bid_qty"], row["best_ask_qty"]), (1.0, 1.0))
close("bid 0.2 %",    row["depth_bid_usd_0p2"], 100.0)   # 1 level x mid 100
close("bid 0.5 %",    row["depth_bid_usd_0p5"], 200.0)
close("bid 1 % (edge level included)", row["depth_bid_usd_1"], 300.0)
close("bid 2 %",      row["depth_bid_usd_2"], 400.0)
close("ask 2 %",      row["depth_ask_usd_2"], 400.0)
close("mark price sets the notional", book(limit=5).aggregate(mark=200)["depth_bid_usd_2"], 800.0)
close("contract size multiplies",
      book(limit=5).aggregate(mark=200, contract_size=0.01)["depth_bid_usd_2"], 8.0)
close("custom bands", book(limit=5).aggregate(bands=[3])["depth_ask_usd_3"], 500.0)
check("depth in whole dollars", book(limit=5).aggregate(mark=100.4)["depth_bid_usd_0p2"], 100)
check("as integers", type(row["depth_bid_usd_2"]), int)

print("== trusted range: a cut-off snapshot is not the whole book")
# The snapshot reached exactly `limit` levels, so it was truncated at the last one.
b = book(bids=BIDS[:3], asks=ASKS[:3], limit=3)
row = b.aggregate()
close("covered bid = distance to the furthest snapshot level", row["covered_bid_pct"], 1.0)
# A delta reveals a level beyond the snapshot. It counts toward depth, but its
# neighbours may still be unknown, so coverage stays where the snapshot left it.
b.apply([["98.5", "4"]], [], 11)
row = b.aggregate()
close("depth counts levels beyond the snapshot", row["depth_bid_usd_2"], 700.0)
close("and those inside it unchanged",           row["depth_bid_usd_1"], 300.0)
close("revealed level does not widen coverage",  row["covered_bid_pct"], 1.0)

print("== a side shorter than the limit is the whole side")
row = book(limit=1000).aggregate()
check("complete side reads as 100 %", (row["covered_bid_pct"], row["covered_ask_pct"]), (100.0, 100.0))
row = book(limit=None).aggregate()
close("unknown limit: assume truncated", row["covered_bid_pct"], 3.0)

print("== price leaving the trusted range")
b = book(bids=BIDS[:3], asks=ASKS[:3], limit=3)      # bids trusted down to 99.0
b.apply([["99.9", "0"], ["99.6", "0"], ["99.0", "0"], ["98.0", "1"]],
        [["98.2", "1"]], 11)                          # market fell through the floor
row = b.aggregate()
check("no coverage below the floor", row["covered_bid_pct"], 0.0)
check("depth still counts what the book holds", row["depth_bid_usd_2"], round(1 * 98.1))

print("== books that must not be aggregated")
check("no snapshot yet",  LocalBook().aggregate(), None)
b = book(limit=5)
b.apply([["100.2", "1"]], [], 11)
check("crossed is detected",   b.crossed(), True)
check("crossed is not aggregated", b.aggregate(), None)
b = book(asks=[], limit=5)
check("empty side is not aggregated", b.aggregate(), None)
b = book(limit=5)
b.invalidate()
check("invalidate drops sync", (b.synced, b.bids, b.asks, b.aggregate()), (False, {}, {}, None))
check("invalidate drops trust", (b.bid_floor, b.ask_ceil), (math.inf, -math.inf))

print("== prune: far levels go, and the trust goes with them")
b = book(limit=1000)                                  # complete on both sides
b.apply([["50", "1"]], [["200", "1"]], 11)
b.prune(2.0)
check("levels beyond 2 % dropped", (50.0 in b.bids, 97.0 in b.bids, 200.0 in b.asks), (False, False, False))
check("levels inside kept",        (98.5 in b.bids, 101.5 in b.asks), (True, True))
row = b.aggregate()
close("complete side now trusted only to the cut", row["covered_bid_pct"], 2.0)
close("same on asks",                              row["covered_ask_pct"], 2.0)
b = book(bids=BIDS[:2], asks=ASKS[:2], limit=2)       # trusted to 0.4 %
b.prune(2.0)
close("prune never widens trust", b.aggregate()["covered_bid_pct"], 0.4)

print("== empty_aggregate: same shape as a real row, all zero")
real, empty = book(limit=5).aggregate(), empty_aggregate()
check("same keys", set(real) ^ set(empty), set())
check("all zero", {v for v in empty.values()}, {0})

print("== absorb: a refreshed book keeps what the old one knew")
old = book(uid=10)                                      # trusted 97.0 .. 103.0
old.apply([["95.0", "2"], ["94.0", "1"], ["99.7", "5"]], [["104.0", "3"]], 11)
fresh = book(bids=[["99.9", "1"], ["99.6", "1"]], asks=ASKS + [["103.5", "1"]], uid=10)
fresh.apply([["95.0", "9"]], [], 11)                    # trusted 99.6 .. 103.5
fresh.absorb(old)
check("old book's trusted levels taken", [p in fresh.bids for p in (99.0, 98.5, 97.0)], [True] * 3)
check("far levels it learned taken", (fresh.bids.get(94.0), fresh.asks.get(104.0)), (1.0, 3.0))
check("a level the fresh book knows is kept", fresh.bids[95.0], 9.0)
check("nothing taken inside the fresh trusted range", 99.7 in fresh.bids, False)
check("trusted range is the union", (fresh.bid_floor, fresh.ask_ceil), (97.0, 103.5))
close("coverage reflects it", fresh.aggregate()["covered_bid_pct"], 3.0)
try:
    book(uid=10).absorb(book(uid=11)); got = None
except ValueError:
    got = "refused"
check("books on different update ids are refused", got, "refused")

print("== determinism: same levels, same row, whatever the arrival order")
a = book(limit=5)
z = book(bids=list(reversed(BIDS)), asks=list(reversed(ASKS)), limit=5)
check("identical rows", a.aggregate(mark=101.3), z.aggregate(mark=101.3))

print()
print("ALL PASSED" if not fails else f"FAILURES: {fails}")
raise SystemExit(1 if fails else 0)
