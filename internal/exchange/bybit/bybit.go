// Package bybit implements the Bybit V5 public spot order book stream.
//
// Channel: orderbook.50.<SYMBOL>. Bybit sends one snapshot after the
// subscription and then only deltas, so the adapter keeps a local book for
// every symbol. A delta with size "0" removes the price level.
//
// Docs: https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook
package bybit

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/w1xent/agregator/internal/exchange"
	"github.com/w1xent/agregator/internal/model"
	"github.com/w1xent/agregator/internal/orderbook"
)

// DefaultURL is the public spot WebSocket endpoint.
const DefaultURL = "wss://stream.bybit.com/v5/public/spot"

const (
	name = "bybit"
	// Bybit accepts at most 10 topics in one subscribe request for spot.
	maxArgsPerRequest = 10
	topicPrefix       = "orderbook.50."
)

// Adapter implements exchange.Adapter for Bybit.
type Adapter struct {
	url string
	// native symbol (BTCUSDT) -> canonical symbol (BTC-USDT)
	symbols map[string]string
	books   map[string]*orderbook.Book
}

// New creates a Bybit adapter. An empty url means DefaultURL.
func New(url string) *Adapter {
	if url == "" {
		url = DefaultURL
	}
	return &Adapter{url: url, symbols: map[string]string{}, books: map[string]*orderbook.Book{}}
}

func (a *Adapter) Name() string        { return name }
func (a *Adapter) URL() string         { return a.url }
func (a *Adapter) PingMessage() []byte { return []byte(`{"op":"ping"}`) }

// Reset forgets all local books. Bybit sends a new snapshot after subscribe.
func (a *Adapter) Reset() { clear(a.books) }

func (a *Adapter) SubscribeMessages(symbols []string) ([][]byte, error) {
	topics := make([]string, 0, len(symbols))
	for _, s := range symbols {
		native := model.NativeSymbol(s)
		a.symbols[native] = model.NormalizeSymbol(s)
		topics = append(topics, topicPrefix+native)
	}
	var msgs [][]byte
	for _, chunk := range exchange.Chunk(topics, maxArgsPerRequest) {
		b, err := json.Marshal(map[string]any{"op": "subscribe", "args": chunk})
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, b)
	}
	return msgs, nil
}

type message struct {
	// Service messages (subscribe result, pong).
	Op      string `json:"op"`
	Success *bool  `json:"success"`
	RetMsg  string `json:"ret_msg"`

	// Data messages.
	Topic string `json:"topic"`
	Type  string `json:"type"` // "snapshot" or "delta"
	TS    int64  `json:"ts"`
	CTS   int64  `json:"cts"`
	Data  struct {
		Symbol string     `json:"s"`
		Bids   [][]string `json:"b"`
		Asks   [][]string `json:"a"`
		Update int64      `json:"u"`
	} `json:"data"`
}

func (a *Adapter) Parse(raw []byte, received time.Time) ([]model.OrderBook, error) {
	var m message
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("bybit: decode: %w", err)
	}
	if m.Op != "" {
		if m.Success != nil && !*m.Success {
			return nil, &exchange.Error{Exchange: name, Code: m.Op, Message: m.RetMsg}
		}
		return nil, nil // subscribe confirmation or pong
	}
	if !strings.HasPrefix(m.Topic, topicPrefix) {
		return nil, nil
	}
	canonical, ok := a.symbols[m.Data.Symbol]
	if !ok {
		return nil, nil
	}

	bids, err := exchange.ParseLevels(m.Data.Bids)
	if err != nil {
		return nil, fmt.Errorf("bybit: %w", err)
	}
	asks, err := exchange.ParseLevels(m.Data.Asks)
	if err != nil {
		return nil, fmt.Errorf("bybit: %w", err)
	}

	book, ok := a.books[m.Data.Symbol]
	switch m.Type {
	case "snapshot":
		if !ok {
			book = orderbook.New()
			a.books[m.Data.Symbol] = book
		}
		book.Reset()
	case "delta":
		if !ok {
			// A delta without a snapshot cannot be applied.
			return nil, fmt.Errorf("bybit: delta before snapshot for %s: %w", m.Data.Symbol, exchange.ErrResync)
		}
	default:
		return nil, nil
	}

	for _, l := range bids {
		book.SetBid(l.Price, l.Size)
	}
	for _, l := range asks {
		book.SetAsk(l.Price, l.Size)
	}
	if book.Crossed() {
		return nil, fmt.Errorf("bybit: crossed book for %s: %w", m.Data.Symbol, exchange.ErrResync)
	}

	// cts is the matching engine time; ts is the time of the push.
	eventMs := m.CTS
	if eventMs == 0 {
		eventMs = m.TS
	}
	return []model.OrderBook{{
		Exchange:   name,
		Symbol:     canonical,
		Bids:       book.Bids(0),
		Asks:       book.Asks(0),
		ExchangeTS: time.UnixMilli(eventMs),
		ReceivedTS: received,
		Seq:        m.Data.Update,
	}}, nil
}
