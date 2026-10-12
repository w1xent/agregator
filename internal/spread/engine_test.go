package spread

import (
	"math"
	"testing"
	"time"

	"github.com/w1xent/agregator/internal/model"
)

var t0 = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

func book(ex string, bid, bidSize, ask, askSize float64, received time.Time) model.OrderBook {
	return model.OrderBook{
		Exchange:   ex,
		Symbol:     "BTC-USDT",
		Bids:       []model.Level{{Price: bid, Size: bidSize}},
		Asks:       []model.Level{{Price: ask, Size: askSize}},
		ExchangeTS: received.Add(-10 * time.Millisecond),
		ReceivedTS: received,
	}
}

func newTestEngine(stale time.Duration) (*Engine, *time.Time) {
	e := NewEngine(stale)
	now := t0
	e.now = func() time.Time { return now }
	return e, &now
}

func find(spreads []model.Spread, buy, sell string) (model.Spread, bool) {
	for _, s := range spreads {
		if s.BuyExchange == buy && s.SellExchange == sell {
			return s, true
		}
	}
	return model.Spread{}, false
}

func TestUpdateComputesBothDirections(t *testing.T) {
	e, _ := newTestEngine(10 * time.Second)

	if got := e.Update(book("bybit", 100, 1, 101, 2, t0)); len(got) != 0 {
		t.Fatalf("a single exchange has no spreads, got %v", got)
	}
	got := e.Update(book("okx", 102, 0.5, 103, 1, t0))
	if len(got) != 2 {
		t.Fatalf("want 2 spreads, got %d", len(got))
	}

	// Buy on bybit at 101, sell on okx at 102: +1, profitable.
	s, ok := find(got, "bybit", "okx")
	if !ok {
		t.Fatal("bybit -> okx spread is missing")
	}
	if s.BuyPrice != 101 || s.SellPrice != 102 || s.Spread != 1 || s.Volume != 0.5 {
		t.Errorf("bad spread %+v", s)
	}
	if want := 1.0 / 101 * 10_000; math.Abs(s.SpreadBps-want) > 1e-9 {
		t.Errorf("bps = %v, want %v", s.SpreadBps, want)
	}

	// Buy on okx at 103, sell on bybit at 100: -3.
	s, ok = find(got, "okx", "bybit")
	if !ok || s.Spread != -3 {
		t.Errorf("okx -> bybit spread = %+v", s)
	}
}

func TestUpdateReturnsOnlyPairsWithUpdatedExchange(t *testing.T) {
	e, _ := newTestEngine(10 * time.Second)
	e.Update(book("bybit", 100, 1, 101, 1, t0))
	e.Update(book("okx", 100, 1, 101, 1, t0))
	got := e.Update(book("bitget", 100, 1, 101, 1, t0))
	if len(got) != 4 {
		t.Fatalf("want 4 spreads (bitget with 2 others, both ways), got %d", len(got))
	}
	for _, s := range got {
		if s.BuyExchange != "bitget" && s.SellExchange != "bitget" {
			t.Errorf("unexpected pair %s -> %s", s.BuyExchange, s.SellExchange)
		}
	}
	if all := e.Spreads(); len(all) != 6 {
		t.Errorf("Spreads() = %d, want 6 for 3 exchanges", len(all))
	}
}

func TestStaleBooksAreIgnored(t *testing.T) {
	e, now := newTestEngine(5 * time.Second)
	e.Update(book("bybit", 100, 1, 101, 1, t0))

	*now = t0.Add(6 * time.Second)
	got := e.Update(book("okx", 102, 1, 103, 1, *now))
	if len(got) != 0 {
		t.Fatalf("stale bybit book must not be used, got %v", got)
	}
	if books := e.Books(); len(books) != 1 || books[0].Exchange != "okx" {
		t.Errorf("Books() = %v, want only okx", books)
	}
	exchanges, symbols := e.Instruments()
	if len(exchanges) != 1 || exchanges[0] != "okx" || len(symbols) != 1 {
		t.Errorf("Instruments() = %v %v; want only the fresh okx book", exchanges, symbols)
	}
}

func TestEmptySideGivesNoSpread(t *testing.T) {
	e, _ := newTestEngine(0)
	e.Update(book("bybit", 100, 1, 101, 1, t0))
	empty := model.OrderBook{Exchange: "okx", Symbol: "BTC-USDT", ReceivedTS: t0}
	if got := e.Update(empty); len(got) != 0 {
		t.Fatalf("got %v", got)
	}
	if q := e.Quotes(); len(q) != 1 {
		t.Errorf("Quotes() = %v, empty book has no quote", q)
	}
}
