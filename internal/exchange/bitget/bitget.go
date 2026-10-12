// Package bitget implements the Bitget V2 public spot order book stream.
//
// Channel: books15. Every message is a full snapshot of the top 15 levels,
// so the adapter does not need to keep any local state.
//
// Docs: https://www.bitget.com/api-doc/spot/websocket/public/Depth-Channel
package bitget

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/w1xent/agregator/internal/exchange"
	"github.com/w1xent/agregator/internal/model"
)

// DefaultURL is the public V2 WebSocket endpoint.
const DefaultURL = "wss://ws.bitget.com/v2/ws/public"

const (
	name              = "bitget"
	channel           = "books15"
	maxArgsPerRequest = 20
)

// Adapter implements exchange.Adapter for Bitget.
type Adapter struct {
	url     string
	symbols map[string]string // BTCUSDT -> BTC-USDT
}

// New creates a Bitget adapter. An empty url means DefaultURL.
func New(url string) *Adapter {
	if url == "" {
		url = DefaultURL
	}
	return &Adapter{url: url, symbols: map[string]string{}}
}

func (a *Adapter) Name() string { return name }
func (a *Adapter) URL() string  { return a.url }

// PingMessage is a plain text "ping". Bitget answers with "pong" and closes
// connections that do not ping for 2 minutes.
func (a *Adapter) PingMessage() []byte { return []byte("ping") }
func (a *Adapter) Reset()              {}

type arg struct {
	InstType string `json:"instType"`
	Channel  string `json:"channel"`
	InstID   string `json:"instId"`
}

func (a *Adapter) SubscribeMessages(symbols []string) ([][]byte, error) {
	args := make([]arg, 0, len(symbols))
	for _, s := range symbols {
		native := model.NativeSymbol(s)
		a.symbols[native] = model.NormalizeSymbol(s)
		args = append(args, arg{InstType: "SPOT", Channel: channel, InstID: native})
	}
	var msgs [][]byte
	for _, chunk := range exchange.Chunk(args, maxArgsPerRequest) {
		b, err := json.Marshal(map[string]any{"op": "subscribe", "args": chunk})
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, b)
	}
	return msgs, nil
}

type message struct {
	Event string          `json:"event"`
	Code  json.RawMessage `json:"code"`
	Msg   string          `json:"msg"`

	Action string `json:"action"`
	Arg    arg    `json:"arg"`
	Data   []struct {
		Asks [][]string `json:"asks"`
		Bids [][]string `json:"bids"`
		TS   string     `json:"ts"`
		Seq  int64      `json:"seq"`
	} `json:"data"`
}

func (a *Adapter) Parse(raw []byte, received time.Time) ([]model.OrderBook, error) {
	if string(raw) == "pong" {
		return nil, nil
	}
	var m message
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("bitget: decode: %w", err)
	}
	if m.Event == "error" {
		return nil, &exchange.Error{Exchange: name, Code: exchange.CodeString(m.Code), Message: m.Msg}
	}
	if m.Event != "" || m.Arg.Channel != channel {
		return nil, nil
	}
	canonical, ok := a.symbols[m.Arg.InstID]
	if !ok {
		return nil, nil
	}

	books := make([]model.OrderBook, 0, len(m.Data))
	for _, d := range m.Data {
		bids, err := exchange.ParseLevels(d.Bids)
		if err != nil {
			return nil, fmt.Errorf("bitget: %w", err)
		}
		asks, err := exchange.ParseLevels(d.Asks)
		if err != nil {
			return nil, fmt.Errorf("bitget: %w", err)
		}
		ts, err := exchange.ParseMillis(d.TS)
		if err != nil {
			return nil, fmt.Errorf("bitget: %w", err)
		}
		books = append(books, model.OrderBook{
			Exchange:   name,
			Symbol:     canonical,
			Bids:       bids,
			Asks:       asks,
			ExchangeTS: ts,
			ReceivedTS: received,
			Seq:        d.Seq,
		})
	}
	return books, nil
}
