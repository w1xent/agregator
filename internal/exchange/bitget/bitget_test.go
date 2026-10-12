package bitget

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/w1xent/agregator/internal/exchange"
	"github.com/w1xent/agregator/internal/model"
)

// Messages are shortened copies of real Bitget messages.
const (
	subscribed = `{"event":"subscribe","arg":{"instType":"SPOT","channel":"books15","instId":"BTCUSDT"}}`
	failed     = `{"event":"error","arg":{"instType":"SPOT","channel":"books15","instId":"FOOUSDT"},"code":30001,"msg":"instId:FOOUSDT doesn't exist","op":"subscribe"}`
	snapshot   = `{"action":"snapshot","arg":{"instType":"SPOT","channel":"books15","instId":"BTCUSDT"},
		"data":[{"asks":[["83504.01","0.186624"],["83504.36","0.000015"]],
		"bids":[["83504","0.65178"],["83498.82","0.005989"]],"checksum":0,"ts":"1791800003500","seq":907861312673}],"ts":1791800003501}`
)

func TestSubscribeMessage(t *testing.T) {
	msgs, err := New("").SubscribeMessages([]string{"BTC-USDT", "eth-usdt"})
	if err != nil || len(msgs) != 1 {
		t.Fatalf("msgs=%d err=%v", len(msgs), err)
	}
	var m struct {
		Op   string `json:"op"`
		Args []arg  `json:"args"`
	}
	if err := json.Unmarshal(msgs[0], &m); err != nil {
		t.Fatal(err)
	}
	want := arg{InstType: "SPOT", Channel: "books15", InstID: "ETHUSDT"}
	if m.Op != "subscribe" || len(m.Args) != 2 || m.Args[1] != want {
		t.Errorf("unexpected message %s", msgs[0])
	}
}

func TestParseSnapshot(t *testing.T) {
	a := New("")
	if _, err := a.SubscribeMessages([]string{"BTC-USDT"}); err != nil {
		t.Fatal(err)
	}
	books, err := a.Parse([]byte(snapshot), time.Now())
	if err != nil || len(books) != 1 {
		t.Fatalf("books=%v err=%v", books, err)
	}
	b := books[0]
	if b.Exchange != "bitget" || b.Symbol != "BTC-USDT" || b.Seq != 907861312673 {
		t.Errorf("bad book header: %+v", b)
	}
	if b.Bids[0] != (model.Level{Price: 83504, Size: 0.65178}) {
		t.Errorf("best bid = %v", b.Bids[0])
	}
	if b.Asks[0] != (model.Level{Price: 83504.01, Size: 0.186624}) {
		t.Errorf("best ask = %v", b.Asks[0])
	}
	if !b.ExchangeTS.Equal(time.UnixMilli(1791800003500)) {
		t.Errorf("ExchangeTS = %v", b.ExchangeTS)
	}
}

func TestServiceMessages(t *testing.T) {
	a := New("")
	for _, raw := range []string{"pong", subscribed} {
		books, err := a.Parse([]byte(raw), time.Now())
		if err != nil || len(books) != 0 {
			t.Errorf("%s: books=%v err=%v", raw, books, err)
		}
	}
	_, err := a.Parse([]byte(failed), time.Now())
	var exErr *exchange.Error
	if !errors.As(err, &exErr) || exErr.Code != "30001" {
		t.Fatalf("err = %v, want exchange error with code 30001", err)
	}
}
