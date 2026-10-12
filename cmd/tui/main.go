// Command tui is a terminal client for the aggregator. It subscribes to the
// gRPC streams and shows prices, order books and spreads in real time.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	pb "github.com/w1xent/agregator/gen/marketdata/v1"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "aggregator gRPC address")
	symbols := flag.String("symbols", "", "comma separated symbols to show (default: all)")
	depth := flag.Int("depth", 5, "order book levels to show")
	flag.Parse()

	if err := run(*addr, *symbols, *depth); err != nil {
		fmt.Fprintln(os.Stderr, "tui:", err)
		os.Exit(1)
	}
}

func run(addr, symbols string, depth int) error {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 20 * time.Second, Timeout: 5 * time.Second}),
	)
	if err != nil {
		return err
	}
	defer conn.Close()
	client := pb.NewMarketDataServiceClient(conn)

	var filter []string
	if symbols != "" {
		filter = strings.Split(symbols, ",")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := newStore()

	go runStream(ctx, st, "books",
		func(ctx context.Context) (interface{ Recv() (*pb.OrderBook, error) }, error) {
			return client.StreamOrderBooks(ctx, &pb.StreamOrderBooksRequest{Symbols: filter, Depth: uint32(depth)})
		},
		func(b *pb.OrderBook) { st.addBook(b, time.Now()) })

	go runStream(ctx, st, "spreads",
		func(ctx context.Context) (interface{ Recv() (*pb.Spread, error) }, error) {
			return client.StreamSpreads(ctx, &pb.StreamSpreadsRequest{Symbols: filter})
		},
		st.addSpread)

	p := tea.NewProgram(model{st: st, addr: addr, depth: depth}, tea.WithAltScreen())
	_, err = p.Run()
	return err
}
