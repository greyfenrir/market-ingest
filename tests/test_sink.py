import asyncio, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ingest.chsink import ClickHouseSink

fails = []
def check(name, got, want):
    if got == want: print(f"  PASS  {name}")
    else: print(f"  FAIL  {name}: got {got!r}, want {want!r}"); fails.append(name)


class FakeResp:
    def __init__(self, status): self.status = status
    async def text(self): return "boom"
    async def __aenter__(self): return self
    async def __aexit__(self, *a): return False


class FakeSession:
    """Records POST bodies; answers with the queued statuses, then 200."""
    def __init__(self, statuses=()):
        self.statuses = list(statuses)
        self.bodies = []
    def post(self, url, params=None, data=None):
        self.bodies.append(data.decode())
        return FakeResp(self.statuses.pop(0) if self.statuses else 200)


def make(**kw):
    return ClickHouseSink("http://x", "u", "p", **kw)


print("== queue overflow drops the OLDEST rows and counts them")
s = make(queue_max_rows=3)
for i in range(5):
    s.write("t", {"i": i})
check("kept newest three", [r["i"] for r in s._queues["t"]], [2, 3, 4])
check("drops counted",     s.dropped_rows, 2)

print("== failed insert is requeued at the front, order intact")
s = make(batch_max_rows=2)
s._session = FakeSession(statuses=[500])
for i in range(4):
    s.write("t", {"i": i})
async def failing_then_ok():
    q = s._queues["t"]
    real = asyncio.sleep  # skip the 1s pause after a failed insert
    asyncio.sleep = lambda *_: real(0)
    try:
        first = await s._flush("t", q)
        after_fail = [r["i"] for r in q]
        second = await s._flush("t", q)
    finally:
        asyncio.sleep = real
    return first, after_fail, second
first, after_fail, second = asyncio.run(failing_then_ok())
check("failure reported",      first, False)
check("error counted",         s.insert_errors, 1)
check("nothing lost, in order", after_fail, [0, 1, 2, 3])
check("retry succeeds",        second, True)
check("retry sent oldest rows", s._session.bodies[1], '{"i":0}\n{"i":1}')
check("inserted counted",      s.inserted_rows, 2)

print()
print("ALL PASSED" if not fails else f"FAILURES: {fails}")
raise SystemExit(1 if fails else 0)
