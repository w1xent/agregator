package history

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/w1xent/agregator/internal/model"
)

type memStore struct {
	mu      sync.Mutex
	spreads []model.Spread
	quotes  []model.Quote
}

func (m *memStore) InsertSpreads(_ context.Context, s []model.Spread) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.spreads = append(m.spreads, s...)
	return nil
}

func (m *memStore) InsertQuotes(_ context.Context, q []model.Quote) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.quotes = append(m.quotes, q...)
	return nil
}

func TestRecorderSamplesAndFlushesOnStop(t *testing.T) {
	store := &memStore{}
	r := New(store, Options{SampleInterval: time.Second, FlushInterval: time.Hour}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	base := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	spread := func(buy string, t time.Time) model.Spread {
		return model.Spread{Symbol: "BTC-USDT", BuyExchange: buy, SellExchange: "okx", Time: t}
	}
	// Same second, same pair: only the first one is kept.
	r.AddSpread(spread("bybit", base.Add(100*time.Millisecond)))
	r.AddSpread(spread("bybit", base.Add(900*time.Millisecond)))
	// Next second: kept.
	r.AddSpread(spread("bybit", base.Add(1100*time.Millisecond)))
	// Other pair in the first second: kept.
	r.AddSpread(spread("bitget", base.Add(500*time.Millisecond)))
	// Quotes follow the same rule.
	r.AddQuote(model.Quote{Exchange: "okx", Symbol: "BTC-USDT", ExchangeTS: base})
	r.AddQuote(model.Quote{Exchange: "okx", Symbol: "BTC-USDT", ExchangeTS: base.Add(10 * time.Millisecond)})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.Run(ctx)
		close(done)
	}()
	// Wait until the queue is empty, then stop: Run must flush on exit.
	deadline := time.Now().Add(2 * time.Second)
	for (len(r.spreadsIn) > 0 || len(r.quotesIn) > 0) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.spreads) != 3 {
		t.Errorf("spreads written = %d, want 3: %+v", len(store.spreads), store.spreads)
	}
	if len(store.quotes) != 1 {
		t.Errorf("quotes written = %d, want 1", len(store.quotes))
	}
}

func TestAddNeverBlocks(t *testing.T) {
	r := New(&memStore{}, Options{QueueSize: 1}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	done := make(chan struct{})
	go func() {
		for range 100 {
			r.AddSpread(model.Spread{})
			r.AddQuote(model.Quote{})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Add blocked on a full queue")
	}
}
