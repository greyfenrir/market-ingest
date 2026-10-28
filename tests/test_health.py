import sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ingest.health import ch_ts, SequenceTracker, StreamStats

fails = []
def check(name, got, want):
    if got == want: print(f"  PASS  {name}")
    else: print(f"  FAIL  {name}: got {got!r}, want {want!r}"); fails.append(name)

print("== ch_ts: ClickHouse DateTime64(3) literal")
check("epoch",            ch_ts(0),              "1970-01-01 00:00:00.000")
check("millis kept",      ch_ts(1789029931123),  "2026-09-10 08:45:31.123")
check("millis zero-pad",  ch_ts(1789029931007),  "2026-09-10 08:45:31.007")
check("float input",      ch_ts(1789029931123.9),"2026-09-10 08:45:31.123")
check("always UTC",       len(ch_ts(time.time()*1000)), 23)

print("== SequenceTracker: only forward jumps are losses")
t = SequenceTracker()
check("first id is not a gap",  t.check("BTC", 100), (0, False))
check("contiguous",             t.check("BTC", 101), (0, False))
check("jump of 1 missing",      t.check("BTC", 103), (1, False))
check("jump of 4 missing",      t.check("BTC", 108), (4, False))
check("duplicate is not a gap", t.check("BTC", 108), (0, False))
check("out-of-order not a gap", t.check("BTC", 105), (0, False))
check("resumes from high water",t.check("BTC", 109), (0, False))
check("per-symbol isolation",   t.check("ETH", 5000), (0, False))

print("== reconnect: the jump across the outage is measured, not forgotten")
t2 = SequenceTracker()
t2.check("BTC", 1000)
t2.mark_reconnect()
check("gap across reconnect is counted", t2.check("BTC", 1051), (50, True))
check("flag clears after first message", t2.check("BTC", 1052), (0, False))
t3 = SequenceTracker()
t3.check("BTC", 10)
t3.check("ETH", 5)
t3.mark_reconnect()
check("clean reconnect, no trades lost", t3.check("BTC", 11), (0, True))
# ETH has not been seen since the reconnect, so its flag must still be pending
# even though BTC has already consumed its own.
check("other symbol keeps its flag",     t3.check("ETH", 7), (1, True))
check("and clears once seen",            t3.check("ETH", 8), (0, False))

print("== StreamStats: minute rollover and percentiles")
st = StreamStats(exchange="x", connector="c", stream="s", session_id="sid")
check("first roll primes only", st.roll(100), None)
for lag in range(1, 101):
    st.observe(nbytes=10, rows=1, lag_ms=lag, symbol="BTC")
check("same minute no row", st.roll(100), None)
row = st.roll(101)
check("rolled a row",     row is not None, True)
check("rows counted",     row["rows"], 100)
check("bytes counted",    row["bytes"], 1000)
check("symbols counted",  row["symbols"], 1)
check("p50",              row["lag_ms_p50"], 51)
check("p99",              row["lag_ms_p99"], 100)
check("max",              row["lag_ms_max"], 100)
check("counters reset",   st.rows, 0)
check("minute literal",   row["minute"], "1970-01-01 01:40:00")

print("== negative lag (clock skew) is clamped, not negative")
st2 = StreamStats(exchange="x", connector="c", stream="s", session_id="sid")
st2.roll(200); st2.observe(nbytes=1, rows=1, lag_ms=-500, symbol="BTC")
check("clamped to zero", st2.roll(201)["lag_ms_p50"], 0)

print()
print("== RollingShare: bad share of a fixed window")
from ingest.health import RollingShare
q = RollingShare(10)
check("empty is 0 %", q.pct(), 0.0)
q.add(True); q.add(True)
check("a start's resync is a share of the whole window", q.pct(), 20.0)
for _ in range(8): q.add(False)
check("window full", q.pct(), 20.0)
q.add(False); q.add(False)
check("old bad seconds slide out", q.pct(), 0.0)
for _ in range(15): q.add(True)
check("never above 100 %", q.pct(), 100.0)

print("ALL PASSED" if not fails else f"FAILURES: {fails}")
raise SystemExit(1 if fails else 0)
