"""Binance USDⓈ-M diff-depth sequencing for one symbol.

Turns `<sym>@depth` events plus a REST snapshot into a LocalBook that is either
provably in step with the exchange or explicitly not in step. The procedure is
the futures one, which differs from Spot in the alignment rule:

    1. buffer events while the snapshot is fetched;
    2. drop buffered events with u < lastUpdateId;
    3. the first applied event must have U <= lastUpdateId <= u;
    4. after that every event's pu must equal the previous event's u.

Any break - a gap at step 3 or a pu mismatch at step 4 - discards the book and
starts over from a fresh snapshot. A book that has drifted cannot be repaired
in place, and a wrong book is worse than an empty one: it produces confident
numbers.

REFRESH WITHOUT A GAP. A snapshot's reach is fixed when it is taken, and the
market walks away from it: on BTC +-0.16 % is gone within minutes, and from
then on one side of the book has no trusted range at all. A refresh builds a
second, shadow DepthSync from a fresh snapshot using the very same procedure,
feeds it the same events as the live book, and swaps it in the moment both
stand on the same update id. The live book never stops in between.

No I/O here. The connector decides when to fetch a snapshot; this class only
says whether it needs one and what to do with it.
"""

from __future__ import annotations

from ..book import LocalBook

SYNCING = "syncing"    # waiting for a snapshot, events go to the buffer
ALIGNING = "aligning"  # snapshot loaded, waiting for the event that spans it
LIVE = "live"          # every event chains onto the previous one by pu


class DepthSync:
    # A snapshot normally lands within a second or two, i.e. a few dozen
    # events at 100 ms. The cap only matters if the REST side is stuck; then
    # the oldest events are the ones a late snapshot would discard anyway.
    MAX_BUFFER = 5000

    def __init__(self, symbol: str, depth_limit: int = 1000) -> None:
        self.symbol = symbol
        self.depth_limit = depth_limit
        self.book = LocalBook()
        self.state = SYNCING
        self.buffer: list[dict] = []
        self.snapshot_pending = False
        # Bumped on every reset. A snapshot requested before a reset belongs
        # to a book that no longer exists; the connector compares generations
        # and throws such a response away instead of loading it.
        self.generation = 0
        self.resyncs = 0
        self.last_event_ms = 0.0      # exchange transaction time of the last applied event
        self.last_applied_mono = 0.0  # local monotonic time of the last applied event
        # Flags for the connector to log and clear: the book went live, or a
        # refresh was swapped in. Kept here so this class stays free of I/O.
        self.just_live = False
        self.just_refreshed = False
        # Refresh: the shadow being built, when the last one started, and the
        # coverage (bid %, ask %) measured on the book's first live aggregate,
        # i.e. how far a fresh snapshot reached.
        self.shadow: DepthSync | None = None
        self.last_refresh_mono = float("-inf")
        self.cover0: tuple[float, float] | None = None
        self._new_second()

    # ------------------------------------------------------------------ api

    @property
    def live(self) -> bool:
        return self.state == LIVE

    @property
    def needs_snapshot(self) -> bool:
        """Only once the stream has buffered something. A snapshot taken
        before the first event can predate the start of the stream, and then
        no event spans it: Binance's own procedure is buffer first, snapshot
        second."""
        return self.state == SYNCING and not self.snapshot_pending and bool(self.buffer)

    def on_event(self, ev: dict, mono: float = 0.0) -> str | None:
        """Feed one depthUpdate. Returns the reason if it broke the sequence;
        the book has then been reset and a new snapshot is needed."""
        if self.state == SYNCING:
            self.buffer.append(ev)
            if len(self.buffer) > self.MAX_BUFFER:
                del self.buffer[: len(self.buffer) - self.MAX_BUFFER]
            return None
        reason = self._step(ev, mono)
        if reason is None and self.shadow is not None:
            # A shadow that breaks just starts over by itself; the live book
            # is unaffected either way.
            self.shadow.on_event(ev, mono)
            self.try_swap()
        return reason

    def on_snapshot(self, snap: dict, mono: float = 0.0) -> str | None:
        """Load a REST /fapi/v1/depth response and replay the buffer onto it.
        Returns a reason if the buffer could not be aligned with it."""
        self.snapshot_pending = False
        last_id = int(snap["lastUpdateId"])
        self.book.load_snapshot(snap.get("bids", []), snap.get("asks", []), last_id,
                                depth_limit=self.depth_limit)
        self.state = ALIGNING
        buffered, self.buffer = self.buffer, []
        for ev in buffered:
            reason = self._step(ev, mono)
            if reason:
                return reason
        return None

    def reset(self, reason: str) -> str:
        """Discard the book. Also called by the connector, on reconnect or on a
        crossed book: either way nothing in it can be trusted any more."""
        # Only a book that had a snapshot loses anything. Resetting one that
        # was still waiting for its first snapshot (every book at startup)
        # must not flag its first live second as degraded.
        if self.state != SYNCING:
            self.resynced = True
        self.book.invalidate()
        self.state = SYNCING
        self.buffer = []
        self.snapshot_pending = False
        self.generation += 1
        self.resyncs += 1
        self.shadow = None      # it was following a sequence that just ended
        self.cover0 = None
        return reason

    def start_refresh(self, mono: float) -> bool:
        """Begin building a shadow book. It buffers from now on and asks for
        its own snapshot (needs_snapshot), exactly as a book does at startup.
        Only one at a time, and only for a live book."""
        if not self.live or self.shadow is not None:
            return False
        self.shadow = DepthSync(self.symbol, self.depth_limit)
        self.last_refresh_mono = mono
        return True

    def try_swap(self) -> bool:
        """Swap the shadow in once it is live and on the same update id as the
        live book. Both apply the same events after alignment, so equal ids
        mean equal books inside the old range - and a wider one outside it."""
        sh = self.shadow
        if sh is None or not sh.live or sh.book.last_update_id != self.book.last_update_id:
            return False
        self.book = sh.book
        self.shadow = None
        self.cover0 = None      # measured again on the next aggregate
        self.just_refreshed = True
        return True

    def take_second(self) -> dict:
        """Counters for the second that just ended, then start a new one."""
        out = {"seq_first": self.seq_first, "seq_last": self.seq_last,
               "n_updates": self.n_updates, "resynced": self.resynced}
        self._new_second()
        return out

    # ------------------------------------------------------------ internals

    def _step(self, ev: dict, mono: float) -> str | None:
        first_id, last_id, prev_id = int(ev["U"]), int(ev["u"]), int(ev["pu"])
        book_id = self.book.last_update_id

        if self.state == ALIGNING:
            if last_id < book_id:
                return None                 # older than the snapshot
            if first_id > book_id:
                return self.reset(f"gap after snapshot: U={first_id} > lastUpdateId={book_id}")
            self.state = LIVE               # U <= lastUpdateId <= u
            self.just_live = True
        else:
            if last_id <= book_id:
                return None                 # already applied (redelivery)
            if prev_id != book_id:
                return self.reset(f"sequence break: pu={prev_id}, expected {book_id}")

        self.book.apply(ev.get("b", []), ev.get("a", []), last_id)
        if not self.seq_first:
            self.seq_first = first_id
        self.seq_last = last_id
        self.n_updates += 1
        self.last_event_ms = float(ev.get("T") or ev.get("E") or 0)
        self.last_applied_mono = mono
        return None

    def _new_second(self) -> None:
        self.seq_first = 0
        self.seq_last = 0
        self.n_updates = 0
        self.resynced = False
