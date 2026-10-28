import asyncio, logging, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ingest.__main__ import run_until_stopped

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

print()
if fails:
    print(f"{len(fails)} FAILED"); sys.exit(1)
print("all passed")
