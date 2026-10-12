// Package storage keeps the history of spreads and quotes in PostgreSQL.
package storage

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/w1xent/agregator/internal/model"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Any random number works; it only has to be the same in every instance.
const migrationLockID = 72_390_001

// Postgres is the history store.
type Postgres struct {
	pool *pgxpool.Pool
}

// Connect opens a connection pool and waits until the database answers
// or ctx is done.
func Connect(ctx context.Context, dsn string, log *slog.Logger) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: %w", err)
	}
	wait := 500 * time.Millisecond
	for {
		err = pool.Ping(ctx)
		if err == nil {
			return &Postgres{pool: pool}, nil
		}
		log.Warn("postgres is not ready, retrying", "error", err, "retry_in", wait.String())
		select {
		case <-ctx.Done():
			pool.Close()
			return nil, fmt.Errorf("postgres: %w", err)
		case <-time.After(wait):
		}
		wait = min(wait*2, 5*time.Second)
	}
}

// Close closes the pool.
func (p *Postgres) Close() { p.pool.Close() }

// Ping checks the connection.
func (p *Postgres) Ping(ctx context.Context) error { return p.pool.Ping(ctx) }

// Migrate applies all embedded migrations that were not applied yet.
// An advisory lock makes it safe to start several instances at once.
func (p *Postgres) Migrate(ctx context.Context) error {
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationLockID); err != nil {
		return fmt.Errorf("lock: %w", err)
	}
	defer conn.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", migrationLockID) //nolint:errcheck

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    TEXT PRIMARY KEY,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return err
	}

	files, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		return err
	}
	slices.Sort(files)
	for _, name := range files {
		var exists bool
		if err := conn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)", name).Scan(&exists); err != nil {
			return err
		}
		if exists {
			continue
		}
		sql, err := migrations.ReadFile(name)
		if err != nil {
			return err
		}
		err = pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, string(sql)); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", name)
			return err
		})
		if err != nil {
			return fmt.Errorf("migration %s: %w", name, err)
		}
	}
	return nil
}

// InsertSpreads writes spreads with the COPY protocol, which is much faster
// than many INSERT statements.
func (p *Postgres) InsertSpreads(ctx context.Context, spreads []model.Spread) error {
	_, err := p.pool.CopyFrom(ctx,
		pgx.Identifier{"spreads"},
		[]string{"ts", "symbol", "buy_exchange", "sell_exchange", "buy_price", "sell_price", "spread", "spread_bps", "volume"},
		pgx.CopyFromSlice(len(spreads), func(i int) ([]any, error) {
			s := &spreads[i]
			return []any{s.Time, s.Symbol, s.BuyExchange, s.SellExchange, s.BuyPrice, s.SellPrice, s.Spread, s.SpreadBps, s.Volume}, nil
		}),
	)
	return err
}

// InsertQuotes writes quotes with the COPY protocol.
func (p *Postgres) InsertQuotes(ctx context.Context, quotes []model.Quote) error {
	_, err := p.pool.CopyFrom(ctx,
		pgx.Identifier{"quotes"},
		[]string{"ts", "exchange", "symbol", "bid", "bid_size", "ask", "ask_size"},
		pgx.CopyFromSlice(len(quotes), func(i int) ([]any, error) {
			q := &quotes[i]
			return []any{q.ExchangeTS, q.Exchange, q.Symbol, q.Bid, q.BidSize, q.Ask, q.AskSize}, nil
		}),
	)
	return err
}

// DeleteOlderThan removes history rows older than cutoff.
func (p *Postgres) DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	var total int64
	for _, table := range []string{"spreads", "quotes"} {
		tag, err := p.pool.Exec(ctx, "DELETE FROM "+table+" WHERE ts < $1", cutoff)
		if err != nil {
			return total, err
		}
		total += tag.RowsAffected()
	}
	return total, nil
}

// SpreadQuery selects spread history.
type SpreadQuery struct {
	Symbol       string
	BuyExchange  string // optional
	SellExchange string // optional
	From, To     time.Time
	// Bucket groups rows into time intervals. 0 returns raw rows.
	Bucket time.Duration
	Limit  int
}

// SpreadPoint is one row of spread history. For raw rows Samples is 1 and
// MinBps == MaxBps == SpreadBps. For buckets the prices are averages.
type SpreadPoint struct {
	Time         time.Time `json:"time"`
	BuyExchange  string    `json:"buy_exchange"`
	SellExchange string    `json:"sell_exchange"`
	BuyPrice     float64   `json:"buy_price"`
	SellPrice    float64   `json:"sell_price"`
	Spread       float64   `json:"spread"`
	SpreadBps    float64   `json:"spread_bps"`
	MinBps       float64   `json:"min_bps"`
	MaxBps       float64   `json:"max_bps"`
	Volume       float64   `json:"volume"`
	Samples      int64     `json:"samples"`
}

const spreadFilter = `
	WHERE symbol = $1 AND ts >= $2 AND ts < $3
	  AND ($4 = '' OR buy_exchange = $4)
	  AND ($5 = '' OR sell_exchange = $5)`

// SpreadHistory returns spread history, newest first.
func (p *Postgres) SpreadHistory(ctx context.Context, q SpreadQuery) ([]SpreadPoint, error) {
	args := []any{q.Symbol, q.From, q.To, q.BuyExchange, q.SellExchange, q.Limit}
	sql := `SELECT ts, buy_exchange, sell_exchange, buy_price, sell_price, spread,
		spread_bps, spread_bps, spread_bps, volume, 1::bigint
		FROM spreads` + spreadFilter + `
		ORDER BY ts DESC LIMIT $6`
	if q.Bucket > 0 {
		args = append(args, interval(q.Bucket))
		sql = `SELECT date_bin($7, ts, TIMESTAMPTZ '2000-01-01 00:00:00+00') AS bucket,
			buy_exchange, sell_exchange, avg(buy_price), avg(sell_price), avg(spread),
			avg(spread_bps), min(spread_bps), max(spread_bps), avg(volume), count(*)
			FROM spreads` + spreadFilter + `
			GROUP BY bucket, buy_exchange, sell_exchange
			ORDER BY bucket DESC, buy_exchange, sell_exchange LIMIT $6`
	}

	rows, err := p.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (SpreadPoint, error) {
		var s SpreadPoint
		err := r.Scan(&s.Time, &s.BuyExchange, &s.SellExchange, &s.BuyPrice, &s.SellPrice, &s.Spread,
			&s.SpreadBps, &s.MinBps, &s.MaxBps, &s.Volume, &s.Samples)
		return s, err
	})
}

// QuoteQuery selects quote history.
type QuoteQuery struct {
	Symbol   string
	Exchange string // optional
	From, To time.Time
	Bucket   time.Duration
	Limit    int
}

// QuotePoint is one row of quote history. For buckets the values are
// averages, and MinMid/MaxMid show the price range inside the bucket.
type QuotePoint struct {
	Time     time.Time `json:"time"`
	Exchange string    `json:"exchange"`
	Bid      float64   `json:"bid"`
	BidSize  float64   `json:"bid_size"`
	Ask      float64   `json:"ask"`
	AskSize  float64   `json:"ask_size"`
	Mid      float64   `json:"mid"`
	MinMid   float64   `json:"min_mid"`
	MaxMid   float64   `json:"max_mid"`
	Samples  int64     `json:"samples"`
}

const quoteFilter = `
	WHERE symbol = $1 AND ts >= $2 AND ts < $3
	  AND ($4 = '' OR exchange = $4)`

// QuoteHistory returns quote history, newest first.
func (p *Postgres) QuoteHistory(ctx context.Context, q QuoteQuery) ([]QuotePoint, error) {
	args := []any{q.Symbol, q.From, q.To, q.Exchange, q.Limit}
	sql := `SELECT ts, exchange, bid, bid_size, ask, ask_size,
		(bid + ask) / 2, (bid + ask) / 2, (bid + ask) / 2, 1::bigint
		FROM quotes` + quoteFilter + `
		ORDER BY ts DESC LIMIT $5`
	if q.Bucket > 0 {
		args = append(args, interval(q.Bucket))
		sql = `SELECT date_bin($6, ts, TIMESTAMPTZ '2000-01-01 00:00:00+00') AS bucket,
			exchange, avg(bid), avg(bid_size), avg(ask), avg(ask_size),
			avg((bid + ask) / 2), min((bid + ask) / 2), max((bid + ask) / 2), count(*)
			FROM quotes` + quoteFilter + `
			GROUP BY bucket, exchange
			ORDER BY bucket DESC, exchange LIMIT $5`
	}

	rows, err := p.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (QuotePoint, error) {
		var q QuotePoint
		err := r.Scan(&q.Time, &q.Exchange, &q.Bid, &q.BidSize, &q.Ask, &q.AskSize,
			&q.Mid, &q.MinMid, &q.MaxMid, &q.Samples)
		return q, err
	})
}

func interval(d time.Duration) pgtype.Interval {
	return pgtype.Interval{Microseconds: d.Microseconds(), Valid: true}
}
