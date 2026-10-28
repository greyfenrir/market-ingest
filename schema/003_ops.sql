-- Operational truth about the archive itself.
--
-- P0's exit criterion is `stale` < 1% and "continuous archive, no gaps > 5 min".
-- You cannot report on that after the fact from the data tables alone: an empty
-- hour in market.trades is indistinguishable from an hour when nothing traded.
-- These tables record what the collector was DOING, so absence of data becomes
-- a measurable fact rather than a guess.

-- Every connect, disconnect, error and stall. A stall is the dangerous one:
-- the socket is open, the exchange is silent, and nothing looks wrong.
CREATE TABLE IF NOT EXISTS ops.connection_events
(
    ts         DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(3)),
    exchange   LowCardinality(String),
    connector  LowCardinality(String),
    stream     LowCardinality(String),
    session_id UUID,
    -- start | open | subscribed | stall | close | error | reconnect | stop
    -- order book: snapshot (loaded) | synced (live) | resync | refresh | rate_limit
    event      LowCardinality(String),
    detail     String CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (exchange, connector, ts)
TTL toDateTime(ts) + INTERVAL 400 DAY;

-- One row per minute per stream: how much arrived and how far behind it was.
-- lag = ingest_time - event_time, i.e. our own staleness against the exchange
-- clock. chrony keeps the server clock honest enough for this to mean something.
CREATE TABLE IF NOT EXISTS ops.stream_health
(
    minute      DateTime('UTC') CODEC(Delta(4), ZSTD(3)),
    exchange    LowCardinality(String),
    connector   LowCardinality(String),
    stream      LowCardinality(String),
    session_id  UUID,
    messages    UInt64 CODEC(ZSTD(3)),
    rows        UInt64 CODEC(ZSTD(3)),
    bytes       UInt64 CODEC(ZSTD(3)),
    symbols     UInt32 CODEC(ZSTD(3)),
    lag_ms_p50  UInt32 CODEC(ZSTD(3)),
    lag_ms_p99  UInt32 CODEC(ZSTD(3)),
    lag_ms_max  UInt32 CODEC(ZSTD(3)),
    gaps        UInt32 CODEC(ZSTD(3)),
    insert_errors UInt32 CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (exchange, connector, stream, minute)
TTL minute + INTERVAL 400 DAY;

-- Proven holes, from sequence-id discontinuities. `missing` is how many
-- aggregate trades we never saw. This is the table that answers "is the
-- archive continuous" with evidence instead of optimism.
CREATE TABLE IF NOT EXISTS ops.sequence_gaps
(
    detected_at DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(3)),
    exchange    LowCardinality(String),
    stream      LowCardinality(String),
    symbol      LowCardinality(String),
    session_id  UUID,
    expected_id UInt64 CODEC(Delta(8), ZSTD(3)),
    received_id UInt64 CODEC(Delta(8), ZSTD(3)),
    missing     UInt64 CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(detected_at)
ORDER BY (exchange, symbol, detected_at);

-- Convenience view: minutes in which a stream produced nothing at all.
-- A row here is either a real market lull or a blind spot; connection_events
-- for the same minute tells you which.
CREATE OR REPLACE VIEW ops.v_silent_minutes AS
SELECT exchange, connector, stream, minute, messages, rows
FROM ops.stream_health
WHERE messages = 0
ORDER BY minute DESC;
