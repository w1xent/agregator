package bybit

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/w1xent/agregator/internal/exchange"
	"github.com/w1xent/agregator/internal/model"
)

// Messages are shortened copies of real Bybit messages.
const (
	subscribeOK = `{"success":true,"ret_msg":"subscribe","conn_id":"x","op":"subscribe"}`
	subscribeKO = `{"success":false,"ret_msg":"Invalid symbol :[orderbook.50.FOOUSDT]","conn_id":"x","op":"subscribe"}`
	pong        = `{"success":true,"ret_msg":"pong","conn_id":"x","op":"ping"}`
	snapshot    = `{"topic":"orderbook.50.BTCUSDT","ts":1791799987476,"type":"snapshot","data":{"s":"BTCUSDT",
		"b":[["83490.6","2.411972"],["83490.5","0.125549"]],
		"a":[["83498.7","0.5"],["83498.8","0.7"]],"u":325407035,"seq":1},"cts":1791799987474}`
	delta = `{"topic":"orderbook.50.BTCUSDT","ts":1791799987516,"type":"delta","data":{"s":"BTCUSDT",
		"b":[["83490.6","2.142504"]],"a":[["83498.7","0.001971"],["83498.8","0"]],"u":325407036,"seq":2},"cts":1791799987514}`
	crossing = `{"topic":"orderbook.50.BTCUSDT","ts":1791799987516,"type":"delta","data":{"s":"BTCUSDT",
		"b":[["83500","1"]],"a":[],"u":325407037,"seq":3},"cts":1791799987514}`
)

func newSubscribed(t *testing.T) *Adapter {
	t.Helper()
	a := New("")
	if _, err := a.SubscribeMessages([]string{"BTC-USDT"}); err != nil {
		t.Fatal(err)
	}
	return a
}

func TestSubscribeMessagesAreChunked(t *testing.T) {
	syms := make([]string, 12)
	for i := range syms {
		syms[i] = "BTC-USDT"
	}
	msgs, err := New("").SubscribeMessages(syms)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 {
		t.Fatalf("got %d messages, want 2 (10 + 2 topics)", len(msgs))
	}
	var m struct {
		Op   string   `json:"op"`
		Args []string `json:"args"`
	}
	if err := json.Unmarshal(msgs[0], &m); err != nil {
		t.Fatal(err)
	}
	if m.Op != "subscribe" || len(m.Args) != 10 || m.Args[0] != "orderbook.50.BTCUSDT" {
		t.Errorf("unexpected message %s", msgs[0])
	}
}

func TestSnapshotThenDelta(t *testing.T) {
	a := newSubscribed(t)
	now := time.Now()

	books, err := a.Parse([]byte(snapshot), now)
	if err != nil || len(books) != 1 {
		t.Fatalf("snapshot: books=%v err=%v", books, err)
	}
	b := books[0]
	if b.Exchange != "bybit" || b.Symbol != "BTC-USDT" {
		t.Errorf("bad identity: %s %s", b.Exchange, b.Symbol)
	}
	if !b.ExchangeTS.Equal(time.UnixMilli(1791799987474)) {
		t.Errorf("ExchangeTS must use cts, got %v", b.ExchangeTS)
	}

	books, err = a.Parse([]byte(delta), now)
	if err != nil || len(books) != 1 {
		t.Fatalf("delta: books=%v err=%v", books, err)
	}
	b = books[0]
	wantBids := []model.Level{{Price: 83490.6, Size: 2.142504}, {Price: 83490.5, Size: 0.125549}}
	wantAsks := []model.Level{{Price: 83498.7, Size: 0.001971}} // 83498.8 was removed
	if len(b.Bids) != 2 || b.Bids[0] != wantBids[0] || b.Bids[1] != wantBids[1] {
		t.Errorf("bids = %v, want %v", b.Bids, wantBids)
	}
	if len(b.Asks) != 1 || b.Asks[0] != wantAsks[0] {
		t.Errorf("asks = %v, want %v", b.Asks, wantAsks)
	}
	if b.Seq != 325407036 {
		t.Errorf("seq = %d", b.Seq)
	}
}

func TestDeltaBeforeSnapshotNeedsResync(t *testing.T) {
	a := newSubscribed(t)
	_, err := a.Parse([]byte(delta), time.Now())
	if !errors.Is(err, exchange.ErrResync) {
		t.Fatalf("err = %v, want ErrResync", err)
	}
}

func TestCrossedBookNeedsResync(t *testing.T) {
	a := newSubscribed(t)
	if _, err := a.Parse([]byte(snapshot), time.Now()); err != nil {
		t.Fatal(err)
	}
	_, err := a.Parse([]byte(crossing), time.Now())
	if !errors.Is(err, exchange.ErrResync) {
		t.Fatalf("err = %v, want ErrResync", err)
	}
}

func TestResetDropsBooks(t *testing.T) {
	a := newSubscribed(t)
	if _, err := a.Parse([]byte(snapshot), time.Now()); err != nil {
		t.Fatal(err)
	}
	a.Reset()
	if _, err := a.Parse([]byte(delta), time.Now()); !errors.Is(err, exchange.ErrResync) {
		t.Fatalf("after Reset a delta must need a new snapshot, err = %v", err)
	}
}

func TestServiceMessages(t *testing.T) {
	a := newSubscribed(t)
	for _, raw := range []string{subscribeOK, pong} {
		books, err := a.Parse([]byte(raw), time.Now())
		if err != nil || len(books) != 0 {
			t.Errorf("%s: books=%v err=%v", raw, books, err)
		}
	}
	_, err := a.Parse([]byte(subscribeKO), time.Now())
	var exErr *exchange.Error
	if !errors.As(err, &exErr) {
		t.Fatalf("err = %v, want *exchange.Error", err)
	}
}
