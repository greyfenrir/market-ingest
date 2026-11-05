"""Bybit v5 orderbook.<N> sequencing for one symbol.

Simpler than Binance, and with a different limit:

- the snapshot arrives on the WebSocket itself, right after subscribing - no
  REST call, nothing to align;
- every delta carries u = previous u + 1. Anything else is a gap, and the only
  repair is a fresh snapshot, i.e. resubscribing the topic;
- a snapshot can come at any time (Bybit resends one after trouble on its side,
  sometimes with u = 1 after a restart) and always replaces the book;
- Bybit keeps the book to its top N levels: a level falling out of the top N is
  deleted by a delta like any other. So the local book is complete from the
  best price out to its N-th level, and holds nothing beyond it - the levels
  learned from deltas that make Binance's depth reach far are never sent here.

No I/O here: the connector resubscribes when told the book needs a snapshot.
"""

from __future__ import annotations

import heapq

from ..book import LocalBook

SYNCING = "syncing"    # waiting for a snapshot
LIVE = "live"


class BybitBook:
    # The venue bounds the book; the collector must not prune it, since the
    # count of levels is what says how far the book is complete.
    bounded = True

    def __init__(self, symbol: str, levels: int = 1000) -> None:
        self.symbol = symbol
        self.levels = levels
        self.book = LocalBook()
        self.state = SYNCING
        self.resyncs = 0
        self.last_event_ms = 0.0      # matching-engine time (cts) of the last update
        self.last_applied_mono = 0.0
        self.live_since_mono = 0.0
        self.syncing_since_mono = 0.0  # when it last lost its book, for the watchdog
        self.just_live = False
        self._new_second()

    @property
    def live(self) -> bool:
        return self.state == LIVE

    def on_message(self, msg: dict, mono: float = 0.0) -> str | None:
        """Feed one orderbook message (snapshot or delta). Returns the reason
        if it broke the sequence; the book has then been reset."""
        d = msg.get("data") or {}
        u = int(d.get("u", 0))
        if msg.get("type") == "snapshot":
            self.book.load_snapshot(d.get("b", []), d.get("a", []), u,
                                    depth_limit=self.levels)
            if self.state != LIVE:
                self.just_live = True
                self.live_since_mono = mono
            self.state = LIVE
        elif self.state != LIVE:
            return None                     # deltas before the snapshot mean nothing
        else:
            expected = self.book.last_update_id + 1
            if u != expected:
                return self.reset(f"sequence break: u={u}, expected {expected}")
            self.book.apply(d.get("b", []), d.get("a", []), u)
        if not self.seq_first:
            self.seq_first = int(d.get("seq", 0))
        self.seq_last = int(d.get("seq", 0))
        self.n_updates += 1
        self.last_event_ms = float(msg.get("cts") or msg.get("ts") or 0)
        self.last_applied_mono = mono
        return None

    def reset(self, reason: str, mono: float = 0.0) -> str:
        if self.state != SYNCING:
            self.resynced = True
        self.book.invalidate()
        self.state = SYNCING
        self.syncing_since_mono = mono
        self.resyncs += 1
        return reason

    def take_second(self) -> dict:
        """Counters for the second that just ended, then start a new one.
        Also re-derives the trusted range, which moves with every delta."""
        if self.live:
            self._retrust()
        out = {"seq_first": self.seq_first, "seq_last": self.seq_last,
               "n_updates": self.n_updates, "resynced": self.resynced}
        self._new_second()
        return out

    # ------------------------------------------------------------ internals

    def _retrust(self) -> None:
        """A side holding the full N levels is complete out to its N-th level;
        a side with fewer is the whole side. Anything beyond the N-th level
        (Bybit should have deleted it) is dropped, so a missed delete cannot
        leave a stale level standing outside what Bybit covers."""
        b = self.book
        if len(b.bids) >= self.levels:
            keep = heapq.nlargest(self.levels, b.bids)
            b.bids = {p: b.bids[p] for p in keep}
            b.bid_floor = keep[-1]
        else:
            b.bid_floor = 0.0
        if len(b.asks) >= self.levels:
            keep = heapq.nsmallest(self.levels, b.asks)
            b.asks = {p: b.asks[p] for p in keep}
            b.ask_ceil = keep[-1]
        else:
            b.ask_ceil = float("inf")

    def _new_second(self) -> None:
        self.seq_first = 0
        self.seq_last = 0
        self.n_updates = 0
        self.resynced = False
