import asyncio, logging, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ingest.rest import HttpError, RateLimited, RestClient

logging.disable(logging.CRITICAL)

fails = []
def check(name, got, want):
    if got == want: print(f"  PASS  {name}")
    else: print(f"  FAIL  {name}: got {got!r}, want {want!r}"); fails.append(name)


class Resp:
    def __init__(self, status, body=None, headers=None):
        self.status, self.body, self.headers = status, body, headers or {}
    async def json(self): return self.body
    async def text(self): return "nope"
    async def __aenter__(self): return self
    async def __aexit__(self, *a): return False

class Session:
    def __init__(self, *resps): self.resps, self.calls = list(resps), []
    def get(self, url, params=None):
        self.calls.append((url, params))
        return self.resps.pop(0) if self.resps else Resp(200, {})


class Clock:
    """Wall clock that only moves when the client sleeps."""
    def __init__(self, t): self.t, self.slept = t, []
    def __call__(self): return self.t

def client(budget=100, *resps, t=6000.0, header="X-USED"):
    clock = Clock(t)
    events = []
    c = RestClient("https://x", budget=budget, used_header=header, wall=clock,
                   on_event=lambda *e: events.append(e))
    c._session = Session(*resps)
    return c, clock, events

def run(clock, coro):
    real = asyncio.sleep
    async def fake(d=0, *a):
        if d: clock.slept.append(round(d, 2)); clock.t += d
        await real(0)
    async def go():
        asyncio.sleep = fake
        try: return await coro
        finally: asyncio.sleep = real
    return asyncio.run(go())


print("== RestClient: calls")
c, clock, events = client(100, Resp(200, {"ok": 1}))
check("200 returns the JSON", run(clock, c.get("/a", {"s": "X"})), {"ok": 1})
check("url and params", c._session.calls, [("https://x/a", {"s": "X"})])

c, clock, events = client(100, Resp(500))
try: run(clock, c.get("/a")); got = None
except HttpError as e: got = e.status
check("non-200 raises with its status", got, 500)

c, clock, events = client(100, Resp(429, headers={"Retry-After": "7"}))
try: run(clock, c.get("/a", stream="depth", what="BTCUSDT")); got = None
except RateLimited: got = "limited"
check("429 raises", got, "limited")
check("and is logged with its wait", events, [("depth", "rate_limit", "BTCUSDT HTTP 429, retry after 7s")])

print("== RestClient: a 429 pauses every caller")
c, clock, events = client()
t = time.monotonic()
c.pause("depth", "BTCUSDT", 429, {"Retry-After": "7"})
check("pause lasts Retry-After", round(c.pause_until - t), 7)
c.pause("open_interest", "ETHUSDT", 418, {"Retry-After": "2"})
check("a shorter pause does not cut a longer one", round(c.pause_until - t), 7)
c.pause("depth", "BTCUSDT", 429, {"Retry-After": "soon"})
check("unreadable Retry-After waits 60 s", round(c.pause_until - t), 60)
c.pause_until = time.monotonic() + 0.2
async def timed_get():
    t0 = time.monotonic(); await c.get("/a"); return time.monotonic() - t0
check("the next call waits out the pause", asyncio.run(timed_get()) >= 0.19, True)
c.pause_until = 0.0
check("no pause, no wait", asyncio.run(timed_get()) < 0.05, True)

print("== RestClient: weight budget per window")
c, clock, events = client(100, t=6000.0)          # window starts at 6000
async def spend(*weights):
    for w in weights: await c.get("/a", weight=w)
run(clock, spend(40, 40))
check("inside the budget: no wait", (clock.slept, c.used()), ([], 80))
run(clock, spend(40))
check("over budget: waits for the next window", clock.slept, [60.1])
check("and counts afresh there", c.used(), 40)

c, clock, events = client(100, Resp(200, {}, headers={"X-USED": "90"}), t=6000.0)
run(clock, spend(1))
check("the venue's count wins over ours", c.used(), 90)
run(clock, spend(20))
check("so the next call waits", clock.slept, [60.1])
c._note_used({"X-USED": "garbage"})
check("unreadable header is ignored", c.used(), 20)

c, clock, events = client(100, t=6030.0)
run(clock, spend(150))
check("a call heavier than the budget still goes, alone", (clock.slept, c.used()), ([], 150))
run(clock, spend(1))
check("then the window is spent", clock.slept, [30.1])

c, clock, events = client(100, t=6000.0, header=None)
c._note_used({"X-USED": "99"})
check("no header configured: ours only", c.used(), 0)

print("== RestClient: a call no longer wanted spends nothing")
c, clock, events = client(100, t=6000.0)
got = run(clock, c.get("/a", weight=20, still_wanted=lambda: False))
check("returns None, no request, no weight", (got, c._session.calls, c.used()), (None, [], 0))

print()
if fails:
    print(f"{len(fails)} FAILED"); sys.exit(1)
print("all passed")
