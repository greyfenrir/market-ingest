import asyncio, datetime, json, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ingest.chsink import ClickHouseSink

fails = []
def check(name, got, want):
    if got == want: print(f"  PASS  {name}")
    else: print(f"  FAIL  {name}: got {got!r}, want {want!r}"); fails.append(name)


class FakeResp:
    def __init__(self, status, raise_exc=None):
        self.status = status
        self.raise_exc = raise_exc
        # ClickHouse names the error in a header; 27 = CANNOT_PARSE_INPUT_ASSERTION_FAILED
        self.headers = {"X-ClickHouse-Exception-Code": "27"} if status == 400 else {}
    async def text(self): return "boom"
    async def __aenter__(self):
        if self.raise_exc: raise self.raise_exc
        return self
    async def __aexit__(self, *a): return False


class FakeSession:
    """Records POST bodies. Answers, in order: the queued statuses (an
    exception instance stands for a network failure), then 400 for any body
    holding a row marked "bad", else 200 - which is what ClickHouse does: one
    bad row and the whole insert is refused."""
    def __init__(self, statuses=()):
        self.statuses = list(statuses)
        self.bodies = []
        self.ok = []        # per body: did "ClickHouse" accept it
    def post(self, url, params=None, data=None):
        body = data.decode()
        self.bodies.append(body)
        if self.statuses:
            s = self.statuses.pop(0)
            resp = FakeResp(0, raise_exc=s) if isinstance(s, Exception) else FakeResp(s)
        else:
            resp = FakeResp(400 if '"bad":true' in body else 200)
        self.ok.append(resp.status == 200)
        return resp


def make(**kw):
    # Column checks have tests of their own, below; FakeSession has no GET.
    kw.setdefault("check_columns", False)
    return ClickHouseSink("http://x", "u", "p", **kw)


def run(coro):
    """Run with the 1 s pause after a failed insert cut to nothing."""
    async def wrapped():
        real = asyncio.sleep
        asyncio.sleep = lambda *_: real(0)
        try:
            return await coro
        finally:
            asyncio.sleep = real
    return asyncio.run(wrapped())


def accepted(session):
    """Row ids that went in, in the order they went in."""
    return [json.loads(line)["i"] for body, ok in zip(session.bodies, session.ok) if ok
            for line in body.split("\n")]


def sink_with(rows, statuses=(), batch=100, **kw):
    s = make(batch_max_rows=batch, **kw)
    s._session = FakeSession(statuses)
    for r in rows:
        s.write("t", r)
    return s


print("== queue overflow drops the OLDEST rows and counts them")
s = make(queue_max_rows=3)
for i in range(5):
    s.write("t", {"i": i})
check("kept newest three", [r["i"] for r in s._queues["t"]], [2, 3, 4])
check("drops counted",     s.dropped_rows, 2)

print("== failed insert is requeued at the front, order intact")
s = sink_with([{"i": i} for i in range(4)], statuses=[500], batch=2)
async def failing_then_ok():
    q = s._queues["t"]
    first = await s._flush("t", q)
    after_fail = [r["i"] for r in q]
    second = await s._flush("t", q)
    return first, after_fail, second
first, after_fail, second = run(failing_then_ok())
check("failure reported",      first, False)
check("error counted",         s.insert_errors, 1)
check("nothing lost, in order", after_fail, [0, 1, 2, 3])
check("retry succeeds",        second, True)
check("retry sent oldest rows", s._session.bodies[1], '{"i":0}\n{"i":1}')
check("inserted counted",      s.inserted_rows, 2)

print("== every failure outside the data waits: 401 403 404 408 500 503, network")
for status in (401, 403, 404, 408, 500, 503, ConnectionResetError("reset")):
    s = sink_with([{"i": 0}, {"i": 1}], statuses=[status])
    settled = run(s._flush("t", s._queues["t"]))
    name = status if isinstance(status, int) else type(status).__name__
    check(f"{name}: requeued, nothing dropped",
          (settled, [r["i"] for r in s._queues["t"]], s.dropped_rows), (False, [0, 1], 0))

print("== 400: one bad row no longer blocks the table")
rows = [{"i": i} for i in range(8)]
rows[5]["bad"] = True
s = sink_with(rows)
settled = run(s._flush("t", s._queues["t"]))
check("batch settled",            settled, True)
check("queue is free",            len(s._queues["t"]), 0)
check("good rows all inserted",   sorted(accepted(s._session)), [0, 1, 2, 3, 4, 6, 7])
check("no good row sent twice",   len(accepted(s._session)), 7)
check("inserted counted",         s.inserted_rows, 7)
check("only the bad row dropped", (s.rejected_rows, s.dropped_rows), (1, 1))
check("one error per rejected batch", s.insert_errors, 1)
# 1 full + 2 per level for log2(8) = 3 levels
check("cheap: 2*log2(n)+1 inserts", len(s._session.bodies), 7)

print("== two bad rows in different halves: every good row still lands")
rows = [{"i": i} for i in range(8)]
rows[0]["bad"] = rows[7]["bad"] = True
s = sink_with(rows)
run(s._flush("t", s._queues["t"]))
check("good rows all inserted",    sorted(accepted(s._session)), [1, 2, 3, 4, 5, 6])
check("only the two bad dropped",  (s.rejected_rows, s.dropped_rows, s.unsalvaged_rows), (2, 2, 0))

print("== scattered bad rows in a big batch, within the limit")
import random
random.seed(7)
for k in (2, 3, 5):
    rows = [{"i": i} for i in range(20000)]
    bad = set(random.sample(range(20000), k))
    for b in bad:
        rows[b]["bad"] = True
    s = sink_with(rows, batch=20000)
    run(s._flush("t", s._queues["t"]))
    got = accepted(s._session)
    check(f"k={k}: every good row in, once",
          (len(got), set(got) == set(range(20000)) - bad), (20000 - k, True))
    check(f"k={k}: only the bad dropped, within the limit",
          (s.rejected_rows, s.unsalvaged_rows, len(s._session.bodies) <= 1 + 128), (k, 0, True))

print("== 400 on every row: the search stops at the limit")
s = sink_with([{"i": i, "bad": True} for i in range(1000)], batch=1000)
run(s._flush("t", s._queues["t"]))
check("all dropped",       (s.rejected_rows, s.dropped_rows, s.inserted_rows), (1000, 1000, 0))
check("1 + 128 inserts",   len(s._session.bodies), 129)
check("counted as unsalvaged: unsearched, not proven bad", s.unsalvaged_rows, 1000)

print("== limit reached with good rows still unsearched")
rows = [{"i": i} for i in range(16)]
rows[0]["bad"] = rows[15]["bad"] = True
s = sink_with(rows, salvage_max_inserts=4)
run(s._flush("t", s._queues["t"]))
# full refused; [0..7] and [8..15] refused; [0..3] refused, [4..7] in; then
# splitting [8..15] would need inserts 5 and 6
check("inserted what the limit reached", sorted(accepted(s._session)), [4, 5, 6, 7])
check("the rest dropped, in order, as unsalvaged",
      (s.dropped_rows, s.rejected_rows, s.unsalvaged_rows), (12, 12, 12))
check("1 + 4 inserts", len(s._session.bodies), 5)

print("== limit 0: a refused batch is dropped whole")
s = sink_with([{"i": 0}, {"i": 1, "bad": True}], salvage_max_inserts=0)
run(s._flush("t", s._queues["t"]))
check("dropped with one request", (s.dropped_rows, s.unsalvaged_rows, len(s._session.bodies)), (2, 2, 1))

print("== 400 on a single-row batch")
s = sink_with([{"i": 0, "bad": True}])
run(s._flush("t", s._queues["t"]))
check("dropped with one request", (s.rejected_rows, len(s._session.bodies)), (1, 1))

print("== a row that is not even JSON is isolated the same way")
s = sink_with([{"i": 0}, {"i": 1, "t": datetime.datetime(2026, 11, 2)}, {"i": 2}, {"i": 3}])
settled = run(s._flush("t", s._queues["t"]))
check("sink survives, batch settled", settled, True)
check("serialisable rows inserted",   sorted(accepted(s._session)), [0, 2, 3])
check("bad row dropped",              s.rejected_rows, 1)

print("== the database failing midway through a salvage loses nothing")
rows = [{"i": i} for i in range(4)]
rows[3]["bad"] = True
# full batch refused, then the left half [0, 1] hits a network error
s = sink_with(rows, statuses=[400, ConnectionResetError("reset")])
settled = run(s._flush("t", s._queues["t"]))
check("not settled",             settled, False)
check("every row back, in order", [r["i"] for r in s._queues["t"]], [0, 1, 2, 3])
check("nothing dropped",          s.dropped_rows, 0)
# full batch refused, left half [0, 1] goes in, right half [2, 3] hits a 503
s = sink_with(rows, statuses=[400, 200, 503])
settled = run(s._flush("t", s._queues["t"]))
check("only the unsettled half back", [r["i"] for r in s._queues["t"]], [2, 3])
check("inserted half counted",        s.inserted_rows, 2)
check("a retry finishes the job",     run(s._flush("t", s._queues["t"])), True)
check("then the bad row is dropped, the good one in",
      (s.rejected_rows, s.inserted_rows, len(s._queues["t"])), (1, 3, 0))
# several refused chunks pending when the database fails: bad 0 and 7 refuse
# the batch, both halves and [0, 1]; [2, 3] goes in; [4, 5] hits a reset
rows = [{"i": i} for i in range(8)]
rows[0]["bad"] = rows[7]["bad"] = True
s = sink_with(rows, statuses=[400, 400, 400, 400, 200, ConnectionResetError("reset")])
settled = run(s._flush("t", s._queues["t"]))
check("not settled",                          settled, False)
check("every unsettled chunk back, in order", [r["i"] for r in s._queues["t"]], [0, 1, 4, 5, 6, 7])
check("inserted chunk not sent again",        (s.inserted_rows, s.dropped_rows), (2, 0))
check("a retry finishes the job",             run(s._flush("t", s._queues["t"])), True)
check("then exactly the two bad are dropped",
      (s.rejected_rows, s.inserted_rows, len(s._queues["t"])), (2, 6, 0))

print("== shutdown drain gets past a rejected batch")
rows = [{"i": i} for i in range(6)]
rows[0]["bad"] = True
s = sink_with(rows, batch=2)
s._stop.set()
run(s._run())
check("drain finished, queue empty", len(s._queues["t"]), 0)
check("rows behind the bad one went in", sorted(accepted(s._session)), [1, 2, 3, 4, 5])

print("== a row key the table has no column for is reported and cut, the row kept")

class TextResp(FakeResp):
    def __init__(self, status, text="", raise_exc=None):
        super().__init__(status, raise_exc)
        self.body = text
    async def text(self): return self.body


class ColumnSession(FakeSession):
    """FakeSession that also answers DESCRIBE TABLE over GET: with `columns`
    as TSV, or with what `describe` holds (a status, or an exception)."""
    def __init__(self, columns, describe=None):
        super().__init__()
        self.columns = list(columns)
        self.describe = describe
        self.queries = []
    def get(self, url, params=None):
        self.queries.append(params["query"])
        d = self.describe
        if isinstance(d, Exception):
            return TextResp(0, raise_exc=d)
        if isinstance(d, int):
            return TextResp(d, "Code: 60. DB::Exception: Table t does not exist")
        return TextResp(200, "".join(f"{c}\tString\t\t\t\t\t\n" for c in self.columns))


def checked(columns, rows, describe=None):
    s = ClickHouseSink("http://x", "u", "p")
    s._session = ColumnSession(columns, describe)
    for r in rows:
        s.write("t", r)
    return s

s = checked(["i", "a"], [{"i": 0, "a": 1}, {"i": 1, "a": 2}])
run(s._flush("t", s._queues["t"]))
check("columns read once, by DESCRIBE", s._session.queries, ["DESCRIBE TABLE t FORMAT TSV"])
check("matching rows go in untouched", s._session.bodies, ['{"i":0,"a":1}\n{"i":1,"a":2}'])
check("nothing reported", (s.unknown, s.unknown_columns()), ({}, 0))
s.write("t", {"i": 2, "a": 3})
run(s._flush("t", s._queues["t"]))
check("known columns are not read again", len(s._session.queries), 1)

s = checked(["i"], [{"i": 0, "new": 1, "newer": 2}, {"i": 1, "new": 3}])
run(s._flush("t", s._queues["t"]))
check("unknown keys reported per table", s.unknown, {"t": {"new", "newer"}})
check("counted for the alarm", s.unknown_columns(), 2)
check("cut from the rows, which land", s._session.bodies, ['{"i":0}\n{"i":1}'])
check("nothing dropped", (s.dropped_rows, s.inserted_rows), (0, 2))
s.write("t", {"i": 2, "new": 4})
run(s._flush("t", s._queues["t"]))
check("not read again within a minute", len(s._session.queries), 1)
check("both still reported, though this batch had one", s.unknown_columns(), 2)

# the migration lands while the collector runs
s._session.columns.append("new")
s._columns_read["t"] -= 61
s.write("t", {"i": 3, "new": 5})
run(s._flush("t", s._queues["t"]))
check("read again after a minute", len(s._session.queries), 2)
check("migrated column now stored", s._session.bodies[-1], '{"i":3,"new":5}')
check("only the migrated key cleared", (s.unknown, s.unknown_columns()), ({"t": {"newer"}}, 1))

for name, describe in (("table missing (404)", 404), ("network down", ConnectionResetError("reset"))):
    s = checked([], [{"i": 0, "x": 1}], describe=describe)
    settled = run(s._flush("t", s._queues["t"]))
    check(f"{name}: rows inserted unchecked", (settled, s._session.bodies), (True, ['{"i":0,"x":1}']))
    check(f"{name}: nothing reported", s.unknown_columns(), 0)
    s.write("t", {"i": 1})
    run(s._flush("t", s._queues["t"]))
    check(f"{name}: a failed read is not retried within a minute", len(s._session.queries), 1)

print("== a key missing from some batches stays reported until the table has it")
s = checked(["i"], [{"i": 0, "opt": 1}])
run(s._flush("t", s._queues["t"]))
s.write("t", {"i": 1})
run(s._flush("t", s._queues["t"]))
check("a batch without it does not clear it", s.unknown_columns(), 1)
check("and is sent as it is", s._session.bodies[-1], '{"i":1}')
s.write("t", {"i": 2, "other": 1})
run(s._flush("t", s._queues["t"]))
check("a second unknown key adds to the first", s.unknown, {"t": {"opt", "other"}})
s._session.columns += ["opt", "other"]
s._columns_read["t"] -= 601
s.write("t", {"i": 3})
run(s._flush("t", s._queues["t"]))
check("cleared once the columns show them", (s.unknown, s.unknown_columns()), ({}, 0))

print("== a column dropped under a running collector is noticed")
s = checked(["i", "gone"], [{"i": 0, "gone": 1}])
run(s._flush("t", s._queues["t"]))
s._session.columns.remove("gone")
s.write("t", {"i": 1, "gone": 2})
run(s._flush("t", s._queues["t"]))
check("not read again within ten minutes", len(s._session.queries), 1)
s._columns_read["t"] -= 601
s.write("t", {"i": 2, "gone": 3})
run(s._flush("t", s._queues["t"]))
check("read again after ten minutes", len(s._session.queries), 2)
check("the dropped key reported and cut", (s.unknown, s._session.bodies[-1]), ({"t": {"gone"}}, '{"i":2}'))

class StrictSession(ColumnSession):
    """Refuses (400) an insert with a key the table has no column for, as
    ClickHouse does with input_format_skip_unknown_fields = 0."""
    def post(self, url, params=None, data=None):
        body = data.decode()
        keys = set().union(*(json.loads(l) for l in body.split("\n")))
        if not keys <= set(self.columns):
            self.bodies.append(body)
            self.ok.append(False)
            return FakeResp(400)
        return super().post(url, params, data)

s = ClickHouseSink("http://x", "u", "p")
s._session = StrictSession(["i", "gone"])
s.write("t", {"i": 0, "gone": 1})
run(s._flush("t", s._queues["t"]))
s._session.columns.remove("gone")
s.write("t", {"i": 1, "gone": 2})
s.write("t", {"i": 2, "gone": 3})
settled = run(s._flush("t", s._queues["t"]))
check("400: columns read again at once", len(s._session.queries), 2)
check("rows fitted and sent again", s._session.bodies[-1], '{"i":1}\n{"i":2}')
check("nothing dropped, nothing salvaged",
      (settled, s.inserted_rows, s.rejected_rows, s.dropped_rows), (True, 3, 0, 0))

rows = [{"i": i} for i in range(4)]
rows[2]["bad"] = True
s = checked(["i", "bad"], rows)
run(s._flush("t", s._queues["t"]))
check("400 the columns do not explain: salvaged as before",
      (sorted(accepted(s._session)), s.rejected_rows), ([0, 1, 3], 1))

print()
print("ALL PASSED" if not fails else f"FAILURES: {fails}")
raise SystemExit(1 if fails else 0)
