package cache

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/w1xent/agregator/internal/model"
)

// Integration test. Run with a real Redis, for example:
//
//	TEST_REDIS_ADDR=localhost:6379 go test ./internal/cache
//
// It uses database 15 to stay away from the running service (database 0).
func TestRedisIntegration(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR is not set")
	}
	ctx := context.Background()
	c := NewRedis(addr, "", 15, time.Minute)
	defer c.Close()
	if err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.rdb.FlushDB(context.Background()) })

	quotes := []model.Quote{
		{Exchange: "okx", Symbol: "BTC-USDT", Bid: 1, Ask: 2},
		{Exchange: "bybit", Symbol: "BTC-USDT", Bid: 1, Ask: 2},
		{Exchange: "okx", Symbol: "ETH-USDT", Bid: 1, Ask: 2},
	}
	spreads := []model.Spread{
		{Symbol: "BTC-USDT", BuyExchange: "okx", SellExchange: "bybit", SpreadBps: -1},
		{Symbol: "BTC-USDT", BuyExchange: "bybit", SellExchange: "okx", SpreadBps: 2},
	}
	if err := c.StoreLatest(ctx, quotes, spreads); err != nil {
		t.Fatal(err)
	}

	gotQ, err := c.LatestQuotes(ctx, "BTC-USDT")
	if err != nil || len(gotQ) != 2 || gotQ[0].Exchange != "bybit" {
		t.Errorf("LatestQuotes = %+v, %v (want 2, sorted by exchange)", gotQ, err)
	}
	gotS, err := c.LatestSpreads(ctx, "")
	if err != nil || len(gotS) != 2 || gotS[0].SpreadBps != 2 {
		t.Errorf("LatestSpreads = %+v, %v (want best first)", gotS, err)
	}

	// A new snapshot replaces the old one completely.
	if err := c.StoreLatest(ctx, quotes[:1], nil); err != nil {
		t.Fatal(err)
	}
	if gotS, _ := c.LatestSpreads(ctx, ""); len(gotS) != 0 {
		t.Errorf("old spreads are still there: %+v", gotS)
	}

	var v struct{ N int }
	if hit, err := c.GetJSON(ctx, "k", &v); hit || err != nil {
		t.Errorf("miss expected, got hit=%v err=%v", hit, err)
	}
	if err := c.SetJSON(ctx, "k", struct{ N int }{7}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if hit, err := c.GetJSON(ctx, "k", &v); !hit || err != nil || v.N != 7 {
		t.Errorf("hit expected, got hit=%v err=%v v=%v", hit, err, v)
	}
}
