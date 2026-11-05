"""One REST client per venue.

Venues limit REST by IP, not by connection, so every REST caller in a
collector has to draw on one budget and stop together once the venue says stop:

- one HTTP session, so keep-alive is shared and nothing opens its own;
- a request-weight budget per fixed window (Binance: per clock minute). The
  venue's own count, when it sends one in a header, wins over ours: it also
  sees other processes on the same IP;
- one pause for every caller after a limit response (Binance 429/418, Bybit
  403), as long as Retry-After asks or, without it, default_pause_s.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Callable

import aiohttp

log = logging.getLogger(__name__)


class RateLimited(Exception):
    """The venue answered with a limit status. The pause is already in force."""


class HttpError(Exception):
    def __init__(self, status: int, text: str) -> None:
        super().__init__(f"HTTP {status}: {text}")
        self.status = status


class RestClient:
    def __init__(self, base_url: str, *, budget: int, window_s: float = 60.0,
                 used_header: str | None = None, limited_statuses=(418, 429),
                 timeout_s: float = 20.0, default_pause_s: float = 60.0,
                 on_event: Callable[[str, str, str], None] | None = None,
                 wall: Callable[[], float] = time.time) -> None:
        self.base_url = base_url.rstrip("/")
        self.budget = budget
        self.window_s = window_s
        self.used_header = used_header
        self.limited_statuses = tuple(limited_statuses)
        self.timeout = aiohttp.ClientTimeout(total=timeout_s)
        self.on_event = on_event
        self.wall = wall
        # How long to stop after a limit response without Retry-After
        self.default_pause_s = default_pause_s
        self._session: aiohttp.ClientSession | None = None
        # FIFO: whoever waits for the budget first is served first
        self._lock = asyncio.Lock()
        self._window = self._window_now()
        self._local = 0     # weight we sent in this window
        self._server = 0    # weight the venue says this IP used in this window
        self.pause_until = 0.0   # monotonic

    # ------------------------------------------------------------ budget

    def _window_now(self) -> int:
        return int(self.wall() // self.window_s)

    def _roll(self) -> None:
        w = self._window_now()
        if w != self._window:
            self._window, self._local, self._server = w, 0, 0

    def used(self) -> int:
        """Weight used in the current window, as far as we know."""
        self._roll()
        return max(self._local, self._server)

    async def _wait_pause(self) -> None:
        while (left := self.pause_until - time.monotonic()) > 0:
            await asyncio.sleep(left)

    async def _acquire(self, weight: int, still_wanted) -> bool:
        async with self._lock:
            while True:
                await self._wait_pause()
                used = self.used()
                # a call heavier than the whole budget still goes, alone
                if used + weight <= self.budget or used == 0:
                    break
                left = (self._window + 1) * self.window_s - self.wall()
                await asyncio.sleep(max(0.0, left) + 0.1)
            if still_wanted is not None and not still_wanted():
                return False
            self._local += weight
            return True

    def _note_used(self, headers) -> None:
        if not self.used_header:
            return
        try:
            v = int(headers.get(self.used_header))
        except (TypeError, ValueError):
            return
        # Clock skew can attribute the venue's last-window count to our new
        # window; that only makes us wait early, never overspend.
        self._roll()
        self._server = max(self._server, v)

    def pause(self, stream: str, what: str, status: int, headers) -> None:
        """Obey a limit response for every caller: venues turn requests sent
        after one into an IP ban, and a ban into a longer one."""
        try:
            wait = float(headers.get("Retry-After") or self.default_pause_s)
        except ValueError:
            wait = self.default_pause_s
        self.pause_until = max(self.pause_until, time.monotonic() + wait)
        if self.on_event:
            self.on_event(stream, "rate_limit", f"{what} HTTP {status}, retry after {wait:.0f}s")
        log.error("REST rate-limited (HTTP %d on %s), pausing all REST for %.0fs",
                  status, what, wait)

    # ------------------------------------------------------------ calls

    async def get(self, path: str, params: dict | None = None, *, weight: int = 1,
                  stream: str = "rest", what: str = "",
                  still_wanted: Callable[[], bool] | None = None):
        """GET and decode JSON. Returns None, spending nothing, if
        still_wanted() turns false while the call waits for its turn.
        Raises RateLimited on a limit status and HttpError on any other non-200."""
        if not await self._acquire(weight, still_wanted):
            return None
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=self.timeout)
        async with self._session.get(self.base_url + path, params=params) as r:
            self._note_used(r.headers)
            if r.status in self.limited_statuses:
                self.pause(stream, what or path, r.status, r.headers)
                raise RateLimited(f"HTTP {r.status}")
            if r.status != 200:
                raise HttpError(r.status, (await r.text())[:200])
            return await r.json()

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None
