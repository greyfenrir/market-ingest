-- Raw exchange feeds.
--
-- BITEMPORALITY IS THE POINT OF THIS FILE.
--
-- Every table carries two clocks:
--   event_time  - when the exchange says the thing happened;
--   ingest_time - when we learned about it.
-- Both are in ORDER BY, so a corrected or re-delivered observation is stored as
-- a NEW ROW beside the old one, never on top of it. "What did we know at 14:00
-- on the 3rd" is then answerable with  WHERE ingest_time <= '...'  which is the
-- whole basis of a backtest that cannot peek into the future.
--
-- This is why these are plain MergeTree tables and NOT ReplacingMergeTree:
-- Replacing collapses rows sharing the sort key, which would silently destroy
-- exactly the revision history we are here to keep. Dedup of genuine duplicates
-- (the same observation delivered twice) is done at query time with LIMIT 1 BY,
-- not by the engine.
--
-- Every table also has `extra`: any field the exchange sends that we do not yet
-- model is kept verbatim as JSON. Binance added `nq` to aggTrade and `ap` to
-- markPrice in the 2026 COIN-M merge; an archive that silently drops unknown
-- fields loses data it can never get back. Empty strings cost nothing.

CREATE TABLE IF NOT EXISTS market.mark_price
(
    exchange          LowCardinality(String),
    symbol            LowCardinality(String),
    event_time        DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(3)),
    ingest_time       DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(3)),
    mark_price        Float64 CODEC(Gorilla, ZSTD(3)),
    index_price       Float64 CODEC(Gorilla, ZSTD(3)),
    settle_price      Float64 CODEC(Gorilla, ZSTD(3)),
    funding_rate      Float64 CODEC(Gorilla, ZSTD(3)),
    next_funding_time DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(3)),
    -- 0 = full-rate tick (focus universe), 1 = one-per-minute archive sample.
    -- Downstream must know whether a gap means "no data" or "lower resolution".
    sampled           UInt8   CODEC(ZSTD(1)),
    extra             String  CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (exchange, symbol, event_time, ingest_time);

CREATE TABLE IF NOT EXISTS market.trades
(
    exchange       LowCardinality(String),
    symbol         LowCardinality(String),
    event_time     DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(3)),
    ingest_time    DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(3)),
    -- Binance: the aggregate-trade id, contiguous per symbol, so a jump in it
    -- is a provable gap in our archive rather than a quiet hole; first_id/
    -- last_id bound the individual trades folded into the aggregate.
    -- Bybit: the venue's cross sequence (order, not contiguity); first_id and
    -- last_id are 0, and the trade's own id is the UUID in trade_id.
    agg_id         UInt64  CODEC(Delta(8), ZSTD(3)),
    -- the venue's trade id when it is not a number (Bybit); empty otherwise.
    -- Added by 005_trade_id.sql on tables created before it.
    trade_id       String  CODEC(ZSTD(3)),
    first_id       UInt64  CODEC(Delta(8), ZSTD(3)),
    last_id        UInt64  CODEC(Delta(8), ZSTD(3)),
    price          Float64 CODEC(Gorilla, ZSTD(3)),
    qty            Float64 CODEC(Gorilla, ZSTD(3)),
    -- true = the buyer was the maker, i.e. the aggressor sold into the bid
    is_buyer_maker UInt8   CODEC(ZSTD(1)),
    extra          String  CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (exchange, symbol, event_time, agg_id);

CREATE TABLE IF NOT EXISTS market.liquidations
(
    exchange        LowCardinality(String),
    symbol          LowCardinality(String),
    event_time      DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(3)),
    ingest_time     DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(3)),
    trade_time      DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(3)),
    side            LowCardinality(String),
    order_type      LowCardinality(String),
    time_in_force   LowCardinality(String),
    order_status    LowCardinality(String),
    price           Float64 CODEC(Gorilla, ZSTD(3)),
    avg_price       Float64 CODEC(Gorilla, ZSTD(3)),
    orig_qty        Float64 CODEC(Gorilla, ZSTD(3)),
    last_filled_qty Float64 CODEC(Gorilla, ZSTD(3)),
    cum_filled_qty  Float64 CODEC(Gorilla, ZSTD(3)),
    extra           String  CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (exchange, symbol, event_time);

-- Open interest has no WebSocket stream on Binance; it is polled over REST.
-- It is also the weight in "median across 9 exchanges weighted by OI", so it is
-- not optional decoration.
-- open_interest is one side (longs = shorts), in the base coin, for every
-- venue. Some venues report both sides summed (Bybit openInterest, Gate
-- total_size, possibly Bitget and HTX): connectors halve or pick the
-- single-side field, and keep the venue's own figures in extra.
CREATE TABLE IF NOT EXISTS market.open_interest
(
    exchange      LowCardinality(String),
    symbol        LowCardinality(String),
    event_time    DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(3)),
    ingest_time   DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(3)),
    open_interest Float64 CODEC(Gorilla, ZSTD(3)),
    extra         String  CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (exchange, symbol, event_time, ingest_time);
