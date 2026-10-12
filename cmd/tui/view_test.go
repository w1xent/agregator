package main

import (
	"os"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/w1xent/agregator/gen/marketdata/v1"
)

func TestViewRendersAllSections(t *testing.T) {
	st := newStore()
	now := time.Now()
	ts := timestamppb.New(now.Add(-50 * time.Millisecond))
	for i, ex := range []string{"bybit", "okx"} {
		off := float64(i) * 0.5
		st.addBook(&pb.OrderBook{
			Exchange: ex, Symbol: "BTC-USDT",
			Bids:         []*pb.PriceLevel{{Price: 83490.5 + off, Size: 1.25}, {Price: 83490.1 + off, Size: 0.5}},
			Asks:         []*pb.PriceLevel{{Price: 83491.0 + off, Size: 0.75}, {Price: 83491.4 + off, Size: 2}},
			ExchangeTime: ts, ReceivedTime: ts, PublishedTime: ts,
		}, now)
	}
	st.addSpread(&pb.Spread{Symbol: "BTC-USDT", BuyExchange: "bybit", SellExchange: "okx",
		BuyPrice: 83491.0, SellPrice: 83491.0, Spread: 0, SpreadBps: 0, Volume: 0.75, Time: ts})
	st.setStream("books", true, nil)
	st.setStream("spreads", true, nil)

	m := model{st: st, addr: "localhost:50051", depth: 3}
	next, _ := m.Update(tickMsg(now))
	view := next.(model).View()

	for _, want := range []string{"LIVE", "BTC-USDT", "Best prices", "Order books", "Cross-exchange spreads", "83491.5", "bybit", "okx"} {
		if !strings.Contains(view, want) {
			t.Errorf("view does not contain %q", want)
		}
	}
	if os.Getenv("SHOW_VIEW") != "" {
		t.Log("\n" + view)
	}
}
