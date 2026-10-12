package aggregator

import (
	"io"
	"log/slog"
	"testing"
	"time"

	pb "github.com/w1xent/agregator/gen/marketdata/v1"
	"github.com/w1xent/agregator/internal/hub"
	"github.com/w1xent/agregator/internal/model"
	"github.com/w1xent/agregator/internal/spread"
)

type countingRecorder struct {
	spreads, quotes int
}

func (r *countingRecorder) AddSpread(model.Spread) { r.spreads++ }
func (r *countingRecorder) AddQuote(model.Quote)   { r.quotes++ }

func TestProcessPublishesBooksSpreadsAndHistory(t *testing.T) {
	now := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	rec := &countingRecorder{}
	s := &Service{
		Engine:     spread.NewEngine(0),
		Books:      hub.New[*pb.OrderBook](10, nil),
		Spreads:    hub.New[*pb.Spread](10, nil),
		Recorder:   rec,
		StaleAfter: 5 * time.Second,
		Log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		now:        func() time.Time { return now },
	}
	books := s.Books.Subscribe(nil)
	spreads := s.Spreads.Subscribe(nil)

	mk := func(ex string, received time.Time) model.OrderBook {
		return model.OrderBook{
			Exchange: ex, Symbol: "BTC-USDT",
			Bids:       []model.Level{{Price: 100, Size: 1}},
			Asks:       []model.Level{{Price: 101, Size: 1}},
			ExchangeTS: received, ReceivedTS: received,
		}
	}

	s.Process(mk("bybit", now))
	s.Process(mk("okx", now.Add(-time.Second)))
	s.Process(mk("bitget", now.Add(-time.Minute))) // stale, must be skipped

	if len(books.C()) != 2 {
		t.Errorf("published books = %d, want 2", len(books.C()))
	}
	if len(spreads.C()) != 2 {
		t.Errorf("published spreads = %d, want 2 (bybit<->okx)", len(spreads.C()))
	}
	if rec.quotes != 2 || rec.spreads != 2 {
		t.Errorf("recorded quotes=%d spreads=%d, want 2 and 2", rec.quotes, rec.spreads)
	}
	b := <-books.C()
	if !b.PublishedTime.AsTime().Equal(now) {
		t.Errorf("published_time = %v, want %v", b.PublishedTime.AsTime(), now)
	}
}

func TestUpdateGaugesRemovesOldPairs(t *testing.T) {
	first := updateGauges(nil, []model.Spread{
		{Symbol: "BTC-USDT", BuyExchange: "a", SellExchange: "b"},
		{Symbol: "BTC-USDT", BuyExchange: "b", SellExchange: "a"},
	})
	if len(first) != 2 {
		t.Fatalf("len = %d", len(first))
	}
	second := updateGauges(first, []model.Spread{{Symbol: "BTC-USDT", BuyExchange: "a", SellExchange: "b"}})
	if len(second) != 1 {
		t.Fatalf("len = %d", len(second))
	}
}
