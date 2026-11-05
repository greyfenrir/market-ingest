import asyncio, logging, re, sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from ingest.__main__ import EXCHANGES, collector_class, exchange_config, run_until_stopped
from ingest.exchanges.base import Collector

logging.disable(logging.CRITICAL)

fails = []
def check(name, got, want):
    if got == want: print(f"  PASS  {name}")
    else: print(f"  FAIL  {name}: got {got!r}, want {want!r}"); fails.append(name)


async def scenario(body, stop_after=None):
    stopping = asyncio.Event()
    task = asyncio.create_task(body())
    if stop_after is not None:
        asyncio.get_running_loop().call_later(stop_after, stopping.set)
    code = await asyncio.wait_for(run_until_stopped(task, stopping), 2)
    return code, task


print("== run_until_stopped: a collector that dies must end the process")

async def crashes():
    await asyncio.sleep(0.01)
    raise KeyError("boom")

async def returns():
    await asyncio.sleep(0.01)

async def forever():
    await asyncio.sleep(3600)

code, _ = asyncio.run(scenario(crashes))
check("crash gives non-zero exit", code, 1)
code, _ = asyncio.run(scenario(returns))
check("unexpected return gives non-zero exit", code, 1)
code, task = asyncio.run(scenario(forever, stop_after=0.01))
check("stop signal exits cleanly", code, 0)
check("and cancels the collector", task.cancelled(), True)

print("== exchanges: every registered name resolves to a collector")
for name in EXCHANGES:
    cls = collector_class(name)
    check(f"{name} is a Collector", issubclass(cls, Collector), True)
    check(f"{name} writes rows as '{name}'", cls.EXCHANGE, name)

print("== config: an exchange runs only with its own section")
check("section returned", exchange_config({"binance_um": {"focus_symbols": ["X"]}}, "binance_um"),
      {"focus_symbols": ["X"]})
for cfg in ({}, {"binance_um": "oops"}):
    try: exchange_config(cfg, "binance_um"); got = None
    except KeyError as e: got = e.args[0]
    check(f"missing or malformed section is an error: {cfg}", got, "no [binance_um] section in the config")

print("== netdata: alarms watch charts that exist, and name no exchange")
charts = (ROOT / "deploy/netdata/market_ingest.conf").read_text()
health = (ROOT / "deploy/netdata/health_market_ingest.conf").read_text()
contexts = set(re.findall(r"^\s*context = (\S+)", charts, re.M))
watched = re.findall(r"^\s*on: (\S+)", health, re.M)
check("every alarm's chart is defined", sorted(set(watched) - contexts), [])
check("every alarm is per dimension", len(re.findall(r"^\s*lookup:.*foreach \*$", health, re.M)), len(watched))
check("no exchange named in alarms", [n for n in EXCHANGES if n in health], [])

print()
if fails:
    print(f"{len(fails)} FAILED"); sys.exit(1)
print("all passed")
