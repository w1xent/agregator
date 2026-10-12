package grpcapi

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/w1xent/agregator/gen/marketdata/v1"
	"github.com/w1xent/agregator/internal/hub"
	"github.com/w1xent/agregator/internal/model"
)

type fakeState struct {
	books   []model.OrderBook
	spreads []model.Spread
}

func (f *fakeState) Books() []model.OrderBook { return f.books }
func (f *fakeState) Spreads() []model.Spread  { return f.spreads }
func (f *fakeState) Instruments() ([]string, []string) {
	return []string{"bybit", "okx"}, []string{"BTC-USDT"}
}

func testBook(ex, sym string) model.OrderBook {
	return model.OrderBook{
		Exchange: ex, Symbol: sym,
		Bids:       []model.Level{{Price: 100, Size: 1}, {Price: 99, Size: 1}, {Price: 98, Size: 1}},
		Asks:       []model.Level{{Price: 101, Size: 1}, {Price: 102, Size: 1}, {Price: 103, Size: 1}},
		ExchangeTS: time.Now(), ReceivedTS: time.Now(),
	}
}

type env struct {
	client  pb.MarketDataServiceClient
	books   *hub.Hub[*pb.OrderBook]
	spreads *hub.Hub[*pb.Spread]
	server  *Server
}

func start(t *testing.T, state State) *env {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	books := hub.New[*pb.OrderBook](16, nil)
	spreads := hub.New[*pb.Spread](16, nil)
	srv := NewServer(state, books, spreads)
	gs := grpc.NewServer()
	pb.RegisterMarketDataServiceServer(gs, srv)
	go gs.Serve(lis) //nolint:errcheck
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return &env{client: pb.NewMarketDataServiceClient(conn), books: books, spreads: spreads, server: srv}
}

// waitSubscribers waits until the server registered the stream in the hub.
func waitSubscribers[T any](t *testing.T, h *hub.Hub[T], n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for h.Len() < n {
		if time.Now().After(deadline) {
			t.Fatalf("subscribers = %d, want %d", h.Len(), n)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestStreamOrderBooksSnapshotFilterAndDepth(t *testing.T) {
	state := &fakeState{books: []model.OrderBook{testBook("bybit", "BTC-USDT"), testBook("okx", "ETH-USDT")}}
	e := start(t, state)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := e.client.StreamOrderBooks(ctx, &pb.StreamOrderBooksRequest{Symbols: []string{"btc/usdt"}, Depth: 2})
	if err != nil {
		t.Fatal(err)
	}

	// 1. Snapshot: only the BTC book, trimmed to 2 levels.
	first, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if first.Symbol != "BTC-USDT" || len(first.Bids) != 2 || len(first.Asks) != 2 {
		t.Fatalf("bad snapshot message: %v", first)
	}

	// 2. Live updates: ETH is filtered out, BTC arrives.
	waitSubscribers(t, e.books, 1)
	pub := timestamppb.Now()
	eth := testBook("okx", "ETH-USDT")
	btc := testBook("okx", "BTC-USDT")
	original := ToProtoBook(&btc, pub)
	e.books.Publish(ToProtoBook(&eth, pub))
	e.books.Publish(original)

	got, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if got.Exchange != "okx" || got.Symbol != "BTC-USDT" || len(got.Bids) != 2 {
		t.Fatalf("bad live message: %v", got)
	}
	if len(original.Bids) != 3 {
		t.Fatal("trimming must not change the shared message")
	}
}

func TestStreamSpreadsMinBps(t *testing.T) {
	e := start(t, &fakeState{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := e.client.StreamSpreads(ctx, &pb.StreamSpreadsRequest{MinBps: proto.Float64(0)})
	if err != nil {
		t.Fatal(err)
	}
	waitSubscribers(t, e.spreads, 1)

	e.spreads.Publish(&pb.Spread{Symbol: "BTC-USDT", BuyExchange: "a", SellExchange: "b", SpreadBps: -1.5})
	e.spreads.Publish(&pb.Spread{Symbol: "BTC-USDT", BuyExchange: "b", SellExchange: "a", SpreadBps: 0.7})

	got, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if got.SpreadBps != 0.7 {
		t.Fatalf("negative spread passed the min_bps filter: %v", got)
	}
}

func TestShutdownEndsStreams(t *testing.T) {
	e := start(t, &fakeState{})
	stream, err := e.client.StreamSpreads(context.Background(), &pb.StreamSpreadsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	waitSubscribers(t, e.spreads, 1)
	e.server.Shutdown()

	errc := make(chan error, 1)
	go func() {
		_, err := stream.Recv()
		errc <- err
	}()
	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("expected end of stream")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stream was not closed on shutdown")
	}
}

func TestListInstruments(t *testing.T) {
	e := start(t, &fakeState{})
	resp, err := e.client.ListInstruments(context.Background(), &pb.ListInstrumentsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Exchanges) != 2 || resp.Symbols[0] != "BTC-USDT" {
		t.Fatalf("unexpected response %v", resp)
	}
}
