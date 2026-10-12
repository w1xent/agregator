// Package okx implements the OKX V5 public order book stream.
//
// Channel: books5. Every message is a full snapshot of the top 5 levels.
// OKX already uses the "BTC-USDT" symbol format, which is our canonical one.
//
// Docs: https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-order-book-channel
package okx

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/w1xent/agregator/internal/exchange"
	"github.com/w1xent/agregator/internal/model"
)

// DefaultURL is the public V5 WebSocket endpoint.
const DefaultURL = "wss://ws.okx.com:8443/ws/v5/public"

const (
	name              = "okx"
	channel           = "books5"
	maxArgsPerRequest = 20
)

// Adapter implements exchange.Adapter for OKX.
type Adapter struct {
	url     string
	symbols map[string]string // OKX instId -> canonical symbol
}

// New creates an OKX adapter. An empty url means DefaultURL.
func New(url string) *Adapter {
	if url == "" {
		url = DefaultURL
	}
	return &Adapter{url: url, symbols: map[string]string{}}
}

func (a *Adapter) Name() string { return name }
func (a *Adapter) URL() string  { return a.url }

// PingMessage is a plain text "ping". OKX closes the connection after
// 30 seconds without any messages, so the ping interval must be shorter.
func (a *Adapter) PingMessage() []byte { return []byte("ping") }
func (a *Adapter) Reset()              {}

type arg struct {
	Channel string `json:"channel"`
	InstID  string `json:"instId"`
}

func (a *Adapter) SubscribeMessages(symbols []string) ([][]byte, error) {
	args := make([]arg, 0, len(symbols))
	for _, s := range symbols {
		inst := model.NormalizeSymbol(s)
		a.symbols[inst] = inst
		args = append(args, arg{Channel: channel, InstID: inst})
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

	Arg  arg `json:"arg"`
	Data []struct {
		Asks  [][]string `json:"asks"`
		Bids  [][]string `json:"bids"`
		TS    string     `json:"ts"`
		SeqID int64      `json:"seqId"`
	} `json:"data"`
}

func (a *Adapter) Parse(raw []byte, received time.Time) ([]model.OrderBook, error) {
	if string(raw) == "pong" {
		return nil, nil
	}
	var m message
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("okx: decode: %w", err)
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
			return nil, fmt.Errorf("okx: %w", err)
		}
		asks, err := exchange.ParseLevels(d.Asks)
		if err != nil {
			return nil, fmt.Errorf("okx: %w", err)
		}
		ts, err := exchange.ParseMillis(d.TS)
		if err != nil {
			return nil, fmt.Errorf("okx: %w", err)
		}
		books = append(books, model.OrderBook{
			Exchange:   name,
			Symbol:     canonical,
			Bids:       bids,
			Asks:       asks,
			ExchangeTS: ts,
			ReceivedTS: received,
			Seq:        d.SeqID,
		})
	}
	return books, nil
}
