package storage

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/w1xent/agregator/internal/model"
)

// Integration test. Run with a real database, for example:
//
//	TEST_POSTGRES_DSN=postgres://md:md@localhost:5432/marketdata?sslmode=disable go test ./internal/storage
func TestPostgresIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pg, err := Connect(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close()
	if err := pg.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := pg.Migrate(ctx); err != nil {
		t.Fatalf("second Migrate must be a no-op: %v", err)
	}

	// A unique symbol and an old time range keep the test away from live data.
	symbol := "IT" + time.Now().Format("150405.000") + "-USDT"
	base := time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)
	t.Cleanup(func() {
		pg.pool.Exec(context.Background(), "DELETE FROM spreads WHERE symbol = $1", symbol) //nolint:errcheck
		pg.pool.Exec(context.Background(), "DELETE FROM quotes WHERE symbol = $1", symbol)  //nolint:errcheck
	})

	var spreads []model.Spread
	var quotes []model.Quote
	for i := range 6 {
		ts := base.Add(time.Duration(i) * 10 * time.Second) // 00:00, 00:10, ... 00:50
		spreads = append(spreads, model.Spread{
			Symbol: symbol, BuyExchange: "bybit", SellExchange: "okx", Time: ts,
			BuyPrice: 100, SellPrice: 101, Spread: 1, SpreadBps: float64(i), Volume: 1,
		})
		quotes = append(quotes, model.Quote{
			Exchange: "okx", Symbol: symbol, ExchangeTS: ts,
			Bid: 100 + float64(i), Ask: 102 + float64(i), BidSize: 1, AskSize: 1,
		})
	}
	if err := pg.InsertSpreads(ctx, spreads); err != nil {
		t.Fatal(err)
	}
	if err := pg.InsertQuotes(ctx, quotes); err != nil {
		t.Fatal(err)
	}

	raw, err := pg.SpreadHistory(ctx, SpreadQuery{Symbol: symbol, From: base, To: base.Add(time.Hour), Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != 3 || raw[0].SpreadBps != 5 || raw[0].Samples != 1 {
		t.Errorf("raw history (newest first, limit 3) = %+v", raw)
	}

	buckets, err := pg.SpreadHistory(ctx, SpreadQuery{Symbol: symbol, BuyExchange: "bybit", From: base, To: base.Add(time.Hour), Bucket: 30 * time.Second, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	// [00:30, 01:00) has bps 3,4,5 and [00:00, 00:30) has 0,1,2.
	if len(buckets) != 2 || buckets[0].Samples != 3 || buckets[0].SpreadBps != 4 || buckets[0].MinBps != 3 || buckets[0].MaxBps != 5 {
		t.Errorf("bucketed history = %+v", buckets)
	}

	none, err := pg.SpreadHistory(ctx, SpreadQuery{Symbol: symbol, SellExchange: "bitget", From: base, To: base.Add(time.Hour), Limit: 10})
	if err != nil || len(none) != 0 {
		t.Errorf("filter by sell exchange: %v %v", none, err)
	}

	qb, err := pg.QuoteHistory(ctx, QuoteQuery{Symbol: symbol, Exchange: "okx", From: base, To: base.Add(time.Hour), Bucket: time.Minute, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	// mid = 101..106, average 103.5
	if len(qb) != 1 || qb[0].Samples != 6 || qb[0].Mid != 103.5 || qb[0].MinMid != 101 || qb[0].MaxMid != 106 {
		t.Errorf("quote buckets = %+v", qb)
	}
}
