// Package model contains the normalized market data types that are shared
// by all services. Every exchange connector converts its own format to these
// types, so the rest of the system does not know about exchange details.
package model

import (
	"strings"
	"time"
)

// Level is one price level of an order book.
type Level struct {
	Price float64 `json:"p"`
	Size  float64 `json:"s"`
}

// OrderBook is a normalized order book snapshot with the top N levels.
//
// Prices are float64. This is fine for analytics and display, but it is not
// precise enough for real order execution.
type OrderBook struct {
	Exchange string `json:"exchange"`
	// Symbol is the canonical symbol, for example "BTC-USDT".
	Symbol string `json:"symbol"`
	// Bids are sorted by price, highest first.
	Bids []Level `json:"bids"`
	// Asks are sorted by price, lowest first.
	Asks []Level `json:"asks"`
	// ExchangeTS is the event time reported by the exchange.
	ExchangeTS time.Time `json:"exchange_ts"`
	// ReceivedTS is the time when the collector received the message.
	ReceivedTS time.Time `json:"received_ts"`
	// Seq is the exchange sequence number (0 if not provided).
	Seq int64 `json:"seq,omitempty"`
}

// BestBid returns the highest bid.
func (b *OrderBook) BestBid() (Level, bool) {
	if len(b.Bids) == 0 {
		return Level{}, false
	}
	return b.Bids[0], true
}

// BestAsk returns the lowest ask.
func (b *OrderBook) BestAsk() (Level, bool) {
	if len(b.Asks) == 0 {
		return Level{}, false
	}
	return b.Asks[0], true
}

// Trim keeps at most depth levels on each side. depth <= 0 keeps everything.
func (b *OrderBook) Trim(depth int) {
	if depth <= 0 {
		return
	}
	if len(b.Bids) > depth {
		b.Bids = b.Bids[:depth]
	}
	if len(b.Asks) > depth {
		b.Asks = b.Asks[:depth]
	}
}

// Quote is the top of the book of one exchange.
type Quote struct {
	Exchange   string    `json:"exchange"`
	Symbol     string    `json:"symbol"`
	Bid        float64   `json:"bid"`
	BidSize    float64   `json:"bid_size"`
	Ask        float64   `json:"ask"`
	AskSize    float64   `json:"ask_size"`
	ExchangeTS time.Time `json:"exchange_ts"`
	ReceivedTS time.Time `json:"received_ts"`
}

// Mid returns the middle price between bid and ask.
func (q Quote) Mid() float64 { return (q.Bid + q.Ask) / 2 }

// QuoteOf builds a Quote from the top of the book.
// It returns false when one side of the book is empty.
func QuoteOf(b *OrderBook) (Quote, bool) {
	bid, okBid := b.BestBid()
	ask, okAsk := b.BestAsk()
	if !okBid || !okAsk {
		return Quote{}, false
	}
	return Quote{
		Exchange:   b.Exchange,
		Symbol:     b.Symbol,
		Bid:        bid.Price,
		BidSize:    bid.Size,
		Ask:        ask.Price,
		AskSize:    ask.Size,
		ExchangeTS: b.ExchangeTS,
		ReceivedTS: b.ReceivedTS,
	}, true
}

// Spread is the result of buying on BuyExchange at its best ask and
// selling on SellExchange at its best bid.
type Spread struct {
	Symbol       string    `json:"symbol"`
	BuyExchange  string    `json:"buy_exchange"`
	SellExchange string    `json:"sell_exchange"`
	BuyPrice     float64   `json:"buy_price"`
	SellPrice    float64   `json:"sell_price"`
	Spread       float64   `json:"spread"`
	SpreadBps    float64   `json:"spread_bps"`
	Volume       float64   `json:"volume"`
	Time         time.Time `json:"time"`
}

// Key returns a stable identifier of the spread direction.
func (s Spread) Key() string {
	return s.Symbol + "|" + s.BuyExchange + "|" + s.SellExchange
}

// NativeSymbol converts a canonical symbol like "BTC-USDT" to the
// "BTCUSDT" form that Bybit and Bitget use.
func NativeSymbol(canonical string) string {
	return strings.ReplaceAll(strings.ToUpper(canonical), "-", "")
}

// NormalizeSymbol makes a canonical symbol: upper case, with a dash between
// base and quote. "btc/usdt" and "btc_usdt" become "BTC-USDT".
func NormalizeSymbol(s string) string {
	s = strings.ToUpper(strings.TrimSpace(s))
	return strings.NewReplacer("/", "-", "_", "-").Replace(s)
}
