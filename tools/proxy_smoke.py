"""Live check of the Binance `proxy` option: starts a minimal HTTP CONNECT
proxy on 127.0.0.1, then opens a real WebSocket and a real REST call to
Binance through the collector's own code paths and checks that both went
through the proxy (and that Basic auth is sent when the URL carries it).

    python tools/proxy_smoke.py

Needs network access to Binance and the project's dependencies."""
import asyncio
import base64
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import websockets  # noqa: E402

from ingest.exchanges.binance_um import REST, WS_MARKET  # noqa: E402
from ingest.rest import RestClient  # noqa: E402

USER, PASSWORD = "u", "p"
seen: list[tuple[str, bool]] = []   # (CONNECT target, auth ok)


async def pipe(r, w):
    try:
        while data := await r.read(65536):
            w.write(data)
            await w.drain()
    except (ConnectionError, asyncio.CancelledError):
        pass
    finally:
        w.close()


async def handle(cr, cw):
    head = (await cr.readuntil(b"\r\n\r\n")).decode()
    lines = head.split("\r\n")
    method, target, _ = lines[0].split(" ", 2)
    want = "Basic " + base64.b64encode(f"{USER}:{PASSWORD}".encode()).decode()
    auth = any(ln.lower().startswith("proxy-authorization:") and ln.split(":", 1)[1].strip() == want
               for ln in lines[1:])
    seen.append((target, auth))
    if method != "CONNECT" or not auth:
        cw.write(b"HTTP/1.1 407 Proxy Authentication Required\r\n\r\n")
        await cw.drain()
        cw.close()
        return
    host, port = target.rsplit(":", 1)
    ur, uw = await asyncio.open_connection(host, int(port))
    cw.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
    await cw.drain()
    await asyncio.gather(pipe(cr, uw), pipe(ur, cw))


async def main() -> int:
    srv = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = srv.sockets[0].getsockname()[1]
    proxy = f"http://{USER}:{PASSWORD}@127.0.0.1:{port}"
    ok = True

    def check(name, cond, detail=""):
        nonlocal ok
        ok &= bool(cond)
        print(f"  {'PASS' if cond else 'FAIL'}  {name} {detail}")

    print(f"proxy on 127.0.0.1:{port}")
    url = WS_MARKET + "/stream?streams=btcusdt@markPrice@1s"
    async with websockets.connect(url, open_timeout=20, proxy=proxy) as ws:
        msg = await asyncio.wait_for(ws.recv(), 10)
    check("ws: message received", "markPriceUpdate" in msg, msg[:60])

    rest = RestClient(REST, budget=100, proxy=proxy)
    try:
        t = await rest.get("/fapi/v1/time")
    finally:
        await rest.close()
    check("rest: /fapi/v1/time", "serverTime" in t, str(t))

    targets = [s[0] for s in seen]
    check("ws went through proxy", "fstream.binance.com:443" in targets)
    check("rest went through proxy", "fapi.binance.com:443" in targets)
    check("Basic auth sent", all(a for _, a in seen), str(seen))

    # wrong password must fail, not silently go direct
    bad = RestClient(REST, budget=100, proxy=f"http://{USER}:x@127.0.0.1:{port}")
    try:
        await bad.get("/fapi/v1/time")
        check("wrong password refused", False)
    except Exception as e:  # noqa: BLE001
        check("wrong password refused", "407" in str(e), f"{type(e).__name__}: {e}"[:80])
    finally:
        await bad.close()

    srv.close()
    print("ALL PASSED" if ok else "FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
