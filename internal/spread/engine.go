// Package spread keeps the latest order book of every exchange and computes
// cross-exchange spreads.
//
// For every ordered pair of exchanges (A, B) and one symbol, the spread is:
//
//	spread     = bestBid(B) - bestAsk(A)   // buy on A, sell on B
//	spread_bps = spread / bestAsk(A) * 10_000
//
// A positive spread means that, ignoring fees, you could buy on A and sell
// on B at the same time with profit.
package spread

import (
	"cmp"
	"slices"
	"sync"
	"time"

	"github.com/w1xent/agregator/internal/model"
)

// Engine stores the latest books and computes spreads. It is safe for
// concurrent use.
type Engine struct {
	staleAfter time.Duration
	now        func() time.Time

	mu    sync.RWMutex
	books map[string]map[string]model.OrderBook // symbol -> exchange -> book
}

// NewEngine creates an engine. Books that were received more than
// staleAfter ago are ignored, so a disconnected exchange does not produce
// fake spreads with an old price.
func NewEngine(staleAfter time.Duration) *Engine {
	return &Engine{
		staleAfter: staleAfter,
		now:        time.Now,
		books:      make(map[string]map[string]model.OrderBook),
	}
}

// Update stores the book and returns the new spreads between its exchange
// and every other exchange with a fresh book of the same symbol, in both
// directions. Pairs without this exchange did not change, so they are not
// returned.
func (e *Engine) Update(b model.OrderBook) []model.Spread {
	e.mu.Lock()
	defer e.mu.Unlock()

	bySymbol, ok := e.books[b.Symbol]
	if !ok {
		bySymbol = make(map[string]model.OrderBook)
		e.books[b.Symbol] = bySymbol
	}
	bySymbol[b.Exchange] = b

	now := e.now()
	var out []model.Spread
	for name, other := range bySymbol {
		if name == b.Exchange || !e.fresh(&other, now) {
			continue
		}
		if s, ok := Calc(&b, &other); ok {
			out = append(out, s)
		}
		if s, ok := Calc(&other, &b); ok {
			out = append(out, s)
		}
	}
	return out
}

// Calc returns the spread of buying on buy (at its best ask) and selling on
// sell (at its best bid).
func Calc(buy, sell *model.OrderBook) (model.Spread, bool) {
	ask, okAsk := buy.BestAsk()
	bid, okBid := sell.BestBid()
	if !okAsk || !okBid || ask.Price <= 0 {
		return model.Spread{}, false
	}
	diff := bid.Price - ask.Price
	t := buy.ExchangeTS
	if sell.ExchangeTS.After(t) {
		t = sell.ExchangeTS
	}
	return model.Spread{
		Symbol:       buy.Symbol,
		BuyExchange:  buy.Exchange,
		SellExchange: sell.Exchange,
		BuyPrice:     ask.Price,
		SellPrice:    bid.Price,
		Spread:       diff,
		SpreadBps:    diff / ask.Price * 10_000,
		Volume:       min(ask.Size, bid.Size),
		Time:         t,
	}, true
}

func (e *Engine) fresh(b *model.OrderBook, now time.Time) bool {
	return e.staleAfter <= 0 || now.Sub(b.ReceivedTS) <= e.staleAfter
}

// Books returns the fresh books, sorted by symbol and exchange.
func (e *Engine) Books() []model.OrderBook {
	e.mu.RLock()
	defer e.mu.RUnlock()
	now := e.now()
	var out []model.OrderBook
	for _, bySymbol := range e.books {
		for _, b := range bySymbol {
			if e.fresh(&b, now) {
				out = append(out, b)
			}
		}
	}
	slices.SortFunc(out, func(a, b model.OrderBook) int {
		return cmp.Or(cmp.Compare(a.Symbol, b.Symbol), cmp.Compare(a.Exchange, b.Exchange))
	})
	return out
}

// Quotes returns the top of every fresh book.
func (e *Engine) Quotes() []model.Quote {
	books := e.Books()
	out := make([]model.Quote, 0, len(books))
	for i := range books {
		if q, ok := model.QuoteOf(&books[i]); ok {
			out = append(out, q)
		}
	}
	return out
}

// Spreads returns the current spreads for all symbols and all pairs of
// exchanges with fresh books.
func (e *Engine) Spreads() []model.Spread {
	books := e.Books() // sorted by symbol, so books of one symbol are neighbours
	var out []model.Spread
	for start := 0; start < len(books); {
		end := start
		for end < len(books) && books[end].Symbol == books[start].Symbol {
			end++
		}
		group := books[start:end]
		for i := range group {
			for j := range group {
				if i == j {
					continue
				}
				if s, ok := Calc(&group[i], &group[j]); ok {
					out = append(out, s)
				}
			}
		}
		start = end
	}
	return out
}

// Instruments returns the exchanges and symbols that have fresh books.
func (e *Engine) Instruments() (exchanges, symbols []string) {
	seenEx, seenSym := map[string]bool{}, map[string]bool{}
	for _, b := range e.Books() {
		if !seenEx[b.Exchange] {
			seenEx[b.Exchange] = true
			exchanges = append(exchanges, b.Exchange)
		}
		if !seenSym[b.Symbol] {
			seenSym[b.Symbol] = true
			symbols = append(symbols, b.Symbol)
		}
	}
	slices.Sort(exchanges)
	slices.Sort(symbols)
	return exchanges, symbols
}
