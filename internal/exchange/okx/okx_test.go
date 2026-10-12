package okx

import (
	"errors"
	"testing"
	"time"

	"github.com/w1xent/agregator/internal/exchange"
	"github.com/w1xent/agregator/internal/model"
)

// Messages are shortened copies of real OKX messages.
const (
	subscribed = `{"event":"subscribe","arg":{"channel":"books5","instId":"BTC-USDT"},"connId":"d958ada6"}`
	failed     = `{"event":"error","msg":"Subscribe failed, instId:FOO-BAR doesn't exist.","code":"60018","connId":"80607b9f"}`
	snapshot   = `{"arg":{"channel":"books5","instId":"BTC-USDT"},"data":[{
		"asks":[["83502.7","2.28986143","0","6"],["83503.3","0.0000102","0","1"]],
		"bids":[["83502.6","3.76734593","0","40"],["83500","0.27460718","0","1"]],
		"instId":"BTC-USDT","ts":"1791799991704","seqId":82269275713}]}`
)

func TestParseSnapshot(t *testing.T) {
	a := New("")
	if _, err := a.SubscribeMessages([]string{"btc-usdt"}); err != nil {
		t.Fatal(err)
	}
	books, err := a.Parse([]byte(snapshot), time.Now())
	if err != nil || len(books) != 1 {
		t.Fatalf("books=%v err=%v", books, err)
	}
	b := books[0]
	if b.Exchange != "okx" || b.Symbol != "BTC-USDT" || b.Seq != 82269275713 {
		t.Errorf("bad book header: %+v", b)
	}
	if b.Bids[0] != (model.Level{Price: 83502.6, Size: 3.76734593}) {
		t.Errorf("best bid = %v", b.Bids[0])
	}
	if b.Asks[1] != (model.Level{Price: 83503.3, Size: 0.0000102}) {
		t.Errorf("second ask = %v", b.Asks[1])
	}
	if !b.ExchangeTS.Equal(time.UnixMilli(1791799991704)) {
		t.Errorf("ExchangeTS = %v", b.ExchangeTS)
	}
}

func TestIgnoresUnknownSymbols(t *testing.T) {
	books, err := New("").Parse([]byte(snapshot), time.Now())
	if err != nil || len(books) != 0 {
		t.Fatalf("books=%v err=%v; data for symbols we did not subscribe must be ignored", books, err)
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
	if !errors.As(err, &exErr) || exErr.Code != "60018" {
		t.Fatalf("err = %v, want exchange error with code 60018", err)
	}
}
