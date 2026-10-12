// Package cache stores the latest market state and short-lived HTTP
// responses in Redis.
//
// Two hashes hold the latest state:
//
//	md:latest:quotes   field "SYMBOL|exchange"      -> JSON model.Quote
//	md:latest:spreads  field "SYMBOL|buy|sell"      -> JSON model.Spread
//
// The aggregator rewrites both hashes a few times per second in one MULTI
// transaction, so readers never see a half written state. The keys have a
// TTL: if the aggregator stops, the "latest" data disappears instead of
// showing old prices.
package cache

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/w1xent/agregator/internal/model"
)

const (
	keyQuotes  = "md:latest:quotes"
	keySpreads = "md:latest:spreads"
)

// Redis is the cache client.
type Redis struct {
	rdb       *redis.Client
	latestTTL time.Duration
}

// NewRedis creates a client. latestTTL is the lifetime of the latest state.
func NewRedis(addr, password string, db int, latestTTL time.Duration) *Redis {
	return &Redis{
		rdb: redis.NewClient(&redis.Options{
			Addr:         addr,
			Password:     password,
			DB:           db,
			DialTimeout:  2 * time.Second,
			ReadTimeout:  time.Second,
			WriteTimeout: time.Second,
		}),
		latestTTL: latestTTL,
	}
}

// Ping checks the connection.
func (c *Redis) Ping(ctx context.Context) error { return c.rdb.Ping(ctx).Err() }

// Close closes the client.
func (c *Redis) Close() error { return c.rdb.Close() }

// StoreLatest replaces the latest quotes and spreads.
func (c *Redis) StoreLatest(ctx context.Context, quotes []model.Quote, spreads []model.Spread) error {
	qFields := make([]any, 0, len(quotes)*2)
	for _, q := range quotes {
		b, err := json.Marshal(q)
		if err != nil {
			return err
		}
		qFields = append(qFields, q.Symbol+"|"+q.Exchange, b)
	}
	sFields := make([]any, 0, len(spreads)*2)
	for _, s := range spreads {
		b, err := json.Marshal(s)
		if err != nil {
			return err
		}
		sFields = append(sFields, s.Key(), b)
	}

	_, err := c.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.Del(ctx, keyQuotes, keySpreads)
		if len(qFields) > 0 {
			p.HSet(ctx, keyQuotes, qFields...)
			p.Expire(ctx, keyQuotes, c.latestTTL)
		}
		if len(sFields) > 0 {
			p.HSet(ctx, keySpreads, sFields...)
			p.Expire(ctx, keySpreads, c.latestTTL)
		}
		return nil
	})
	return err
}

// LatestQuotes returns the latest quotes. An empty symbol means all symbols.
func (c *Redis) LatestQuotes(ctx context.Context, symbol string) ([]model.Quote, error) {
	out, err := readHash[model.Quote](ctx, c.rdb, keyQuotes, symbol)
	slices.SortFunc(out, func(a, b model.Quote) int {
		return cmp.Or(cmp.Compare(a.Symbol, b.Symbol), cmp.Compare(a.Exchange, b.Exchange))
	})
	return out, err
}

// LatestSpreads returns the latest spreads, best first. An empty symbol
// means all symbols.
func (c *Redis) LatestSpreads(ctx context.Context, symbol string) ([]model.Spread, error) {
	out, err := readHash[model.Spread](ctx, c.rdb, keySpreads, symbol)
	slices.SortFunc(out, func(a, b model.Spread) int {
		return cmp.Or(cmp.Compare(a.Symbol, b.Symbol), cmp.Compare(b.SpreadBps, a.SpreadBps))
	})
	return out, err
}

func readHash[T any](ctx context.Context, rdb *redis.Client, key, symbol string) ([]T, error) {
	all, err := rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	out := make([]T, 0, len(all))
	for field, raw := range all {
		if symbol != "" && !strings.HasPrefix(field, symbol+"|") {
			continue
		}
		var v T
		if err := json.Unmarshal([]byte(raw), &v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// GetJSON reads a cached JSON value into dst. It returns false on a miss.
func (c *Redis) GetJSON(ctx context.Context, key string, dst any) (bool, error) {
	raw, err := c.rdb.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, json.Unmarshal(raw, dst)
}

// SetJSON stores v as JSON with a TTL.
func (c *Redis) SetJSON(ctx context.Context, key string, v any, ttl time.Duration) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.rdb.Set(ctx, key, raw, ttl).Err()
}
