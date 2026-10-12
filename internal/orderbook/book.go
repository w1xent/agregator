// Package orderbook keeps a local copy of an order book that is built from
// a snapshot and a stream of incremental updates (deltas).
package orderbook

import (
	"cmp"
	"slices"

	"github.com/w1xent/agregator/internal/model"
)

// Book is a local order book. It is not safe for concurrent use.
type Book struct {
	bids map[float64]float64
	asks map[float64]float64
}

// New returns an empty book.
func New() *Book {
	return &Book{bids: make(map[float64]float64), asks: make(map[float64]float64)}
}

// Reset removes all levels.
func (b *Book) Reset() {
	clear(b.bids)
	clear(b.asks)
}

// SetBid sets the size of a bid level. Size 0 removes the level.
func (b *Book) SetBid(price, size float64) { set(b.bids, price, size) }

// SetAsk sets the size of an ask level. Size 0 removes the level.
func (b *Book) SetAsk(price, size float64) { set(b.asks, price, size) }

func set(side map[float64]float64, price, size float64) {
	if size <= 0 {
		delete(side, price)
		return
	}
	side[price] = size
}

// Bids returns up to depth bids, highest price first. depth <= 0 means all.
func (b *Book) Bids(depth int) []model.Level {
	return levels(b.bids, depth, func(x, y model.Level) int { return cmp.Compare(y.Price, x.Price) })
}

// Asks returns up to depth asks, lowest price first. depth <= 0 means all.
func (b *Book) Asks(depth int) []model.Level {
	return levels(b.asks, depth, func(x, y model.Level) int { return cmp.Compare(x.Price, y.Price) })
}

func levels(side map[float64]float64, depth int, less func(x, y model.Level) int) []model.Level {
	out := make([]model.Level, 0, len(side))
	for p, s := range side {
		out = append(out, model.Level{Price: p, Size: s})
	}
	slices.SortFunc(out, less)
	if depth > 0 && len(out) > depth {
		out = out[:depth]
	}
	return out
}

// Empty reports whether both sides have no levels.
func (b *Book) Empty() bool { return len(b.bids) == 0 && len(b.asks) == 0 }

// Crossed reports whether the best bid is higher than or equal to the best ask.
// A crossed book means that the local copy is broken and must be rebuilt.
func (b *Book) Crossed() bool {
	if len(b.bids) == 0 || len(b.asks) == 0 {
		return false
	}
	bestBid := 0.0
	for p := range b.bids {
		bestBid = max(bestBid, p)
	}
	for p := range b.asks {
		if p <= bestBid {
			return true
		}
	}
	return false
}
