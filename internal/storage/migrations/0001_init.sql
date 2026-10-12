-- Spreads between two exchanges, sampled (by default once per second per pair).
CREATE TABLE IF NOT EXISTS spreads (
    ts            TIMESTAMPTZ      NOT NULL,
    symbol        TEXT             NOT NULL,
    buy_exchange  TEXT             NOT NULL,
    sell_exchange TEXT             NOT NULL,
    buy_price     DOUBLE PRECISION NOT NULL,
    sell_price    DOUBLE PRECISION NOT NULL,
    spread        DOUBLE PRECISION NOT NULL,
    spread_bps    DOUBLE PRECISION NOT NULL,
    volume        DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS spreads_symbol_ts_idx ON spreads (symbol, ts DESC);
CREATE INDEX IF NOT EXISTS spreads_ts_idx ON spreads (ts);

-- Top of the book of one exchange, sampled (by default once per second).
CREATE TABLE IF NOT EXISTS quotes (
    ts       TIMESTAMPTZ      NOT NULL,
    exchange TEXT             NOT NULL,
    symbol   TEXT             NOT NULL,
    bid      DOUBLE PRECISION NOT NULL,
    bid_size DOUBLE PRECISION NOT NULL,
    ask      DOUBLE PRECISION NOT NULL,
    ask_size DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS quotes_symbol_ts_idx ON quotes (symbol, ts DESC);
CREATE INDEX IF NOT EXISTS quotes_ts_idx ON quotes (ts);
