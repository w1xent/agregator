// Package grpcapi implements the MarketDataService gRPC server.
package grpcapi

import (
	"context"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/w1xent/agregator/gen/marketdata/v1"
	"github.com/w1xent/agregator/internal/hub"
	"github.com/w1xent/agregator/internal/model"
)

var (
	metricStreams = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "md_grpc_active_streams",
		Help: "Open gRPC streams.",
	}, []string{"method"})
	metricSent = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "md_grpc_messages_sent_total",
		Help: "Messages sent to gRPC clients.",
	}, []string{"method"})
)

// State gives the current market state for new subscribers.
type State interface {
	Books() []model.OrderBook
	Spreads() []model.Spread
	Instruments() (exchanges, symbols []string)
}

// Server implements pb.MarketDataServiceServer.
type Server struct {
	pb.UnimplementedMarketDataServiceServer

	state   State
	books   *hub.Hub[*pb.OrderBook]
	spreads *hub.Hub[*pb.Spread]
	done    chan struct{}
}

// NewServer creates the gRPC service.
func NewServer(state State, books *hub.Hub[*pb.OrderBook], spreads *hub.Hub[*pb.Spread]) *Server {
	return &Server{state: state, books: books, spreads: spreads, done: make(chan struct{})}
}

// Shutdown ends all open streams. Streams never finish on their own, so
// grpc.Server.GracefulStop would wait forever without this.
func (s *Server) Shutdown() { close(s.done) }

func (s *Server) ListInstruments(context.Context, *pb.ListInstrumentsRequest) (*pb.ListInstrumentsResponse, error) {
	exchanges, symbols := s.state.Instruments()
	return &pb.ListInstrumentsResponse{Exchanges: exchanges, Symbols: symbols}, nil
}

func (s *Server) StreamOrderBooks(req *pb.StreamOrderBooksRequest, stream grpc.ServerStreamingServer[pb.OrderBook]) error {
	const method = "StreamOrderBooks"
	symbols := toSet(req.GetSymbols(), model.NormalizeSymbol)
	exchanges := toSet(req.GetExchanges(), strings.ToLower)
	depth := int(req.GetDepth())
	match := func(b *pb.OrderBook) bool {
		return matches(symbols, b.Symbol) && matches(exchanges, b.Exchange)
	}

	// Subscribe before reading the snapshot, so no update is lost between
	// them. A client may get one update twice, which is harmless because
	// every message is a full book.
	sub := s.books.Subscribe(match)
	defer s.books.Unsubscribe(sub)
	metricStreams.WithLabelValues(method).Inc()
	defer metricStreams.WithLabelValues(method).Dec()

	now := timestamppb.Now()
	for _, b := range s.state.Books() {
		msg := ToProtoBook(&b, now)
		if !match(msg) {
			continue
		}
		if err := stream.Send(trimBook(msg, depth)); err != nil {
			return err
		}
		metricSent.WithLabelValues(method).Inc()
	}

	return pump(stream.Context(), s.done, sub, func(b *pb.OrderBook) error {
		metricSent.WithLabelValues(method).Inc()
		return stream.Send(trimBook(b, depth))
	})
}

func (s *Server) StreamSpreads(req *pb.StreamSpreadsRequest, stream grpc.ServerStreamingServer[pb.Spread]) error {
	const method = "StreamSpreads"
	symbols := toSet(req.GetSymbols(), model.NormalizeSymbol)
	match := func(sp *pb.Spread) bool {
		if req.MinBps != nil && sp.SpreadBps < req.GetMinBps() {
			return false
		}
		return matches(symbols, sp.Symbol)
	}

	sub := s.spreads.Subscribe(match)
	defer s.spreads.Unsubscribe(sub)
	metricStreams.WithLabelValues(method).Inc()
	defer metricStreams.WithLabelValues(method).Dec()

	now := timestamppb.Now()
	for _, sp := range s.state.Spreads() {
		msg := ToProtoSpread(&sp, now)
		if !match(msg) {
			continue
		}
		if err := stream.Send(msg); err != nil {
			return err
		}
		metricSent.WithLabelValues(method).Inc()
	}

	return pump(stream.Context(), s.done, sub, func(sp *pb.Spread) error {
		metricSent.WithLabelValues(method).Inc()
		return stream.Send(sp)
	})
}

// pump sends messages from the subscription until the client leaves or the
// server shuts down.
func pump[T any](ctx context.Context, done <-chan struct{}, sub *hub.Subscription[T], send func(T) error) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-done:
			return nil
		case v := <-sub.C():
			if err := send(v); err != nil {
				return err
			}
		}
	}
}

func toSet(items []string, norm func(string) string) map[string]bool {
	if len(items) == 0 {
		return nil
	}
	set := make(map[string]bool, len(items))
	for _, it := range items {
		set[norm(strings.TrimSpace(it))] = true
	}
	return set
}

// matches returns true for an empty (nil) set, which means "everything".
func matches(set map[string]bool, v string) bool { return set == nil || set[v] }
