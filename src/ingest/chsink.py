"""Batched, back-pressured writer into ClickHouse over the HTTP interface.

Deliberately dependency-light: JSONEachRow over plain HTTP is enough, and one
less native protocol library is one less thing to debug at 3am.

Two properties matter more than throughput here:

1. A collector must never die because the database is briefly unavailable.
   Rows queue in memory; if the queue fills we drop the OLDEST rows and count
   the loss, because an OOM-killed collector loses everything, not just a
   window. Dropped rows are counted in dropped_rows (alarmed in Netdata).

2. Batches are per-table. ClickHouse hates many small inserts (each one is a
   part that has to be merged later), so we buffer to batch_max_rows or
   batch_max_seconds, whichever comes first.

3. A failed insert is retried only if retrying can help. HTTP 400 means
   ClickHouse refused the data itself (a value it cannot parse, a wrong type,
   out of range): the same rows get the same answer forever, and retrying them
   would block the table while its queue overflows. Such a batch is split to
   save its good rows, and the rest is dropped and counted. Every other failure
   - network, timeouts, overload, authentication (401/403), a table not created
   yet (404) - is fixed outside the data, so those rows wait and are retried.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from typing import Any

import aiohttp

log = logging.getLogger(__name__)

# ClickHouse answers 400 only for defects of the request itself: parse, type,
# range and syntax errors (exceptionCodeToHTTPStatus.cpp). Not for missing
# tables (404), access (401/403), timeouts (408) or load (500/503).
REJECTED = 400


class _Retry(Exception):
    """The insert failed for a reason outside the data; the rows can wait."""


class ClickHouseSink:
    def __init__(
        self,
        url: str,
        user: str,
        password: str,
        *,
        batch_max_rows: int = 5000,
        batch_max_seconds: float = 2.0,
        queue_max_rows: int = 400_000,
    ) -> None:
        self._url = url.rstrip("/")
        self._auth = aiohttp.BasicAuth(user, password)
        self._batch_max_rows = batch_max_rows
        self._batch_max_seconds = batch_max_seconds
        self._queue_max_rows = queue_max_rows

        self._queues: dict[str, deque[dict[str, Any]]] = {}
        self._session: aiohttp.ClientSession | None = None
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()

        self.dropped_rows = 0     # lost for any reason: overflow or rejected
        self.rejected_rows = 0    # of those, refused by ClickHouse (HTTP 400)
        self.insert_errors = 0
        self.inserted_rows = 0

    # ------------------------------------------------------------------ api

    def write(self, table: str, row: dict[str, Any]) -> None:
        """Queue one row. Never blocks, never raises."""
        q = self._queues.get(table)
        if q is None:
            q = self._queues[table] = deque()
        q.append(row)
        if len(q) > self._queue_max_rows:
            # Oldest first: recent data is what monitoring and live views need.
            overflow = len(q) - self._queue_max_rows
            for _ in range(overflow):
                q.popleft()
            self.dropped_rows += overflow
            log.error("sink queue overflow on %s, dropped %d rows", table, overflow)

    def queued_rows(self) -> int:
        """Rows waiting to be inserted. Growth here is the earliest visible
        sign that ClickHouse is struggling, well before anything errors."""
        return sum(len(q) for q in self._queues.values())

    async def start(self) -> None:
        timeout = aiohttp.ClientTimeout(total=30)
        self._session = aiohttp.ClientSession(timeout=timeout, auth=self._auth)
        self._task = asyncio.create_task(self._run(), name="ch-sink")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            await self._task
        if self._session:
            await self._session.close()

    async def ping(self) -> None:
        """Fail fast at startup rather than discovering the problem under load."""
        assert self._session is not None
        async with self._session.get(f"{self._url}/ping") as r:
            r.raise_for_status()

    # -------------------------------------------------------------- internals

    async def _run(self) -> None:
        last_flush = time.monotonic()
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=0.25)
            except asyncio.TimeoutError:
                pass
            now = time.monotonic()
            due_by_time = (now - last_flush) >= self._batch_max_seconds
            for table in list(self._queues):
                q = self._queues[table]
                if not q:
                    continue
                if due_by_time or len(q) >= self._batch_max_rows:
                    await self._flush(table, q)
            if due_by_time:
                last_flush = now
        # drain what we can on the way out
        for table in list(self._queues):
            q = self._queues[table]
            while q:
                if not await self._flush(table, q):
                    break

    async def _flush(self, table: str, q: deque) -> bool:
        """Insert one batch from the front of q. True if the batch is settled
        (inserted, or rejected and dropped), False if it went back to wait."""
        n = min(len(q), self._batch_max_rows)
        rows = [q.popleft() for _ in range(n)]
        try:
            error = await self._insert(table, rows)
            if error is None:
                return True
            self.insert_errors += 1
            log.error("insert into %s rejected (%d rows), salvaging: %s", table, n, error)
            await self._salvage(table, rows, error)
            return True
        except _Retry as exc:
            self.insert_errors += 1
            # Put them back at the FRONT so ordering survives a transient failure.
            left = exc.args[1]
            q.extendleft(reversed(left))
            log.warning("insert into %s failed (%d rows requeued): %s", table, len(left), exc.args[0])
            await asyncio.sleep(1.0)
            return False

    async def _insert(self, table: str, rows: list) -> str | None:
        """One INSERT. None when it went in, ClickHouse's message when it was
        rejected (HTTP 400). Raises _Retry for anything else."""
        assert self._session is not None
        try:
            body = "\n".join(json.dumps(r, separators=(",", ":")) for r in rows)
        except (TypeError, ValueError) as exc:
            # A row that cannot even be serialised is as bad as one ClickHouse
            # refuses, and must not take the sink task down with it.
            return f"not serialisable: {exc}"
        params = {"query": f"INSERT INTO {table} FORMAT JSONEachRow"}
        try:
            async with self._session.post(self._url, params=params, data=body.encode()) as r:
                status = r.status
                if status == 200:
                    self.inserted_rows += len(rows)
                    return None
                text = (await r.text())[:400].strip()
                code = r.headers.get("X-ClickHouse-Exception-Code", "?")
        except Exception as exc:  # noqa: BLE001 - a sink must not propagate
            raise _Retry(f"{type(exc).__name__}: {exc}", rows) from None
        error = f"HTTP {status}, code {code}: {text}"
        if status != REJECTED:
            raise _Retry(error, rows)
        return error

    async def _salvage(self, table: str, rows: list, error: str) -> None:
        """ClickHouse refuses a whole insert for one bad row. Halve the batch
        and insert each half: a stray bad row costs about 2*log2(n) small
        inserts, and every good row still lands. When both halves of a split
        are refused, the defect is in the rows as a kind (a type that no
        longer fits the table), not in one of them: the rest is dropped then,
        rather than spending a request per row.

        Raises _Retry with the rows still unsettled if the database fails for
        another reason midway; rows already inserted are not sent again."""
        chunk = rows
        while len(chunk) > 1:
            mid = len(chunk) // 2
            halves = [chunk[:mid], chunk[mid:]]
            refused = []
            for k, half in enumerate(halves):
                try:
                    e = await self._insert(table, half)
                except _Retry as exc:
                    unsettled = [r for h in refused for r in h] + [r for h in halves[k:] for r in h]
                    raise _Retry(exc.args[0], unsettled) from None
                if e is not None:
                    refused.append(half)
                    error = e
            if not refused:
                return
            if len(refused) == 2:
                break
            chunk = refused[0]
        self._drop_rejected(table, chunk, error)

    def _drop_rejected(self, table: str, rows: list, error: str) -> None:
        self.rejected_rows += len(rows)
        self.dropped_rows += len(rows)
        sample = json.dumps(rows[0], separators=(",", ":"), default=repr)[:400]
        log.error("dropped %d rows rejected by %s: %s; first row: %s",
                  len(rows), table, error, sample)
