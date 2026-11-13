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
   would block the table while its queue overflows. Such a batch is split, up
   to salvage_max_inserts inserts, to save its good rows; the bad rows, and
   whatever the limit left unsearched, are dropped and counted. Every other failure
   - network, timeouts, overload, authentication (401/403), a table not created
   yet (404) - is fixed outside the data, so those rows wait and are retried.

4. A row key the table has no column for is reported, not silently lost.
   With input_format_skip_unknown_fields = 1 (the default on our servers)
   ClickHouse drops such a key without a word: code released ahead of its
   migration would write nothing into its new column, and nobody would know.
   Before a table's first insert, again when a key it has not seen arrives,
   every COLUMNS_TTL_S, and after an insert rejected with 400 (a column
   dropped or renamed), the sink reads the table's columns (DESCRIBE TABLE).
   A key that is
   not among them is logged, counted in unknown_columns (alarmed in Netdata)
   and cut from the rows, so the rows themselves land whatever the server's
   setting. If the columns cannot be read, rows are inserted unchecked.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from collections import deque
from typing import Any

import aiohttp

log = logging.getLogger(__name__)

# ClickHouse answers 400 only for defects of the request itself: parse, type,
# range and syntax errors (exceptionCodeToHTTPStatus.cpp). Not for missing
# tables (404), access (401/403), timeouts (408) or load (500/503).
REJECTED = 400
# Columns of a table are read again at most this often, when a key arrives
# that they do not have: a migration applied while the collector runs is seen.
COLUMNS_RECHECK_S = 60.0
# ...and at least this often anyway: a column dropped or renamed under a
# running collector loses its values silently too.
COLUMNS_TTL_S = 600.0


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
        salvage_max_inserts: int = 128,
        check_columns: bool = True,
    ) -> None:
        self._url = url.rstrip("/")
        self._auth = aiohttp.BasicAuth(user, password)
        self._batch_max_rows = batch_max_rows
        self._batch_max_seconds = batch_max_seconds
        self._queue_max_rows = queue_max_rows
        # Inserts spent isolating the bad rows of one refused batch; 0 drops
        # a refused batch whole. Each insert that lands is a part to merge.
        self._salvage_max_inserts = max(0, int(salvage_max_inserts))

        self._queues: dict[str, deque[dict[str, Any]]] = {}
        self._session: aiohttp.ClientSession | None = None
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()

        self.dropped_rows = 0     # lost for any reason: overflow or rejected
        self.rejected_rows = 0    # of those, refused by ClickHouse (HTTP 400)
        self.unsalvaged_rows = 0  # of those, not searched for bad rows (limit hit)
        self.insert_errors = 0
        self.inserted_rows = 0

        self._check_columns = check_columns
        self._columns: dict[str, set[str]] = {}     # table -> its columns, as last read
        self._columns_read: dict[str, float] = {}   # table -> monotonic time of that read
        # table -> keys the rows carry that the table has no column for
        self.unknown: dict[str, set[str]] = {}

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

    def unknown_columns(self) -> int:
        """Row keys, over all tables, that ClickHouse has no column for."""
        return sum(len(k) for k in self.unknown.values())

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
        if self._check_columns:
            rows = await self._fit_columns(table, rows)
        try:
            error = await self._insert(table, rows)
            if error is not None and self._check_columns:
                # Maybe the schema changed under us: fit the rows to the
                # columns as they are now, and if that cut anything, try again.
                fitted = await self._fit_columns(table, rows, force=True)
                if fitted is not rows:
                    log.warning("insert into %s rejected, retrying with the columns "
                                "read again: %s", table, error)
                    rows = fitted
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

    async def _fit_columns(self, table: str, rows: list, force: bool = False) -> list:
        """The rows with every key the table has no column for cut out, and
        the loss reported. The very list passed in comes back when nothing
        was cut, or while the table's columns are unknown. force reads the
        columns again whatever the limits."""
        keys = set().union(*rows)
        cols = self._columns.get(table)
        mono = time.monotonic()
        age = mono - self._columns_read.get(table, -math.inf)
        if (force or age >= COLUMNS_TTL_S
                or ((cols is None or not keys <= cols) and age >= COLUMNS_RECHECK_S)):
            self._columns_read[table] = mono
            fresh = await self._read_columns(table)
            if fresh is not None:
                self._columns[table] = cols = fresh
        if cols is None:
            return rows
        before = self.unknown.get(table, set())
        new = keys - cols - before
        # A key stays reported until the table's columns show it: a batch
        # that happens not to carry it proves nothing.
        unknown = (before - cols) | new
        if new:
            log.error("%s has no column for %s: these values are NOT stored until the "
                      "schema is migrated (the rows themselves are)", table, sorted(new))
        if before - unknown:
            log.warning("%s now has a column for %s", table, sorted(before - unknown))
        if not unknown:
            self.unknown.pop(table, None)
            return rows
        self.unknown[table] = unknown
        if keys.isdisjoint(unknown):
            return rows
        return [{k: v for k, v in r.items() if k not in unknown} for r in rows]

    async def _read_columns(self, table: str) -> set[str] | None:
        """The table's column names, or None if they cannot be read now."""
        assert self._session is not None
        params = {"query": f"DESCRIBE TABLE {table} FORMAT TSV"}
        try:
            async with self._session.get(self._url, params=params) as r:
                text = await r.text()
                status = r.status
        except Exception as exc:  # noqa: BLE001 - checking must never stop the insert
            log.warning("cannot read the columns of %s: %s: %s", table, type(exc).__name__, exc)
            return None
        if status != 200:
            log.error("cannot read the columns of %s (HTTP %d): %s", table, status, text[:300])
            return None
        return {line.split("\t", 1)[0] for line in text.splitlines() if line}

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
        """ClickHouse refuses a whole insert for one bad row. Halve every
        refused chunk and insert both halves, until each bad row is alone:
        k bad rows cost about 2*k*log2(n/k) inserts, and every good row lands.
        Bad rows in both halves of a split mean nothing about the others, so
        every refused half is split again, not just one.

        Chunks are taken level by level, largest first, so the good rows of
        big halves land before the search goes deep. It stops after
        salvage_max_inserts inserts: when every row is bad (a type that no
        longer fits the table) a full search would cost ~2n requests. What is
        left unsearched then is dropped, good rows with it, and counted in
        unsalvaged_rows as well as rejected_rows.

        Raises _Retry with the rows still unsettled, in their original order,
        if the database fails for another reason midway; rows already inserted
        are not sent again."""
        pending = deque([(0, rows)])    # (offset in rows, refused chunk)
        bad: list = []                  # rows refused on their own
        inserts = 0
        while pending:
            at, chunk = pending.popleft()
            if len(chunk) == 1:
                bad.extend(chunk)
                continue
            if inserts + 2 > self._salvage_max_inserts:
                pending.appendleft((at, chunk))
                break
            mid = len(chunk) // 2
            halves = [(at, chunk[:mid]), (at + mid, chunk[mid:])]
            for k, (off, half) in enumerate(halves):
                try:
                    e = await self._insert(table, half)
                except _Retry as exc:
                    if bad:
                        self._drop_rejected(table, bad, error)
                    left = sorted([*pending, *halves[k:]], key=lambda c: c[0])
                    raise _Retry(exc.args[0], [r for _, c in left for r in c]) from None
                inserts += 1
                if e is not None:
                    pending.append((off, half))
                    error = e
        if bad:
            self._drop_rejected(table, bad, error)
        if pending:
            left = [r for _, c in sorted(pending, key=lambda c: c[0]) for r in c]
            self.unsalvaged_rows += len(left)
            self._drop_rejected(table, left, error,
                                f"of {table} left unsearched after {inserts} salvage inserts "
                                f"(the limit), good ones among them")

    def _drop_rejected(self, table: str, rows: list, error: str, why: str = "") -> None:
        self.rejected_rows += len(rows)
        self.dropped_rows += len(rows)
        sample = json.dumps(rows[0], separators=(",", ":"), default=repr)[:400]
        log.error("dropped %d rows %s: %s; first row: %s",
                  len(rows), why or f"rejected by {table}", error, sample)
