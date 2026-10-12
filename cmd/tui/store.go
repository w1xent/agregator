package main

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/status"

	pb "github.com/w1xent/agregator/gen/marketdata/v1"
)

// store keeps the latest data received from the gRPC streams. The streams
// write to it, and the UI reads a snapshot several times per second.
type store struct {
	received atomic.Int64

	mu       sync.Mutex
	books    map[string]map[string]*pb.OrderBook // symbol -> exchange -> book
	spreads  map[string]map[string]*pb.Spread    // symbol -> "buy|sell" -> spread
	streams  map[string]streamState
	e2e      latencyWindow // exchange event -> this client
	delivery latencyWindow // aggregator publish -> this client
}

type streamState struct {
	connected bool
	err       string
}

func newStore() *store {
	return &store{
		books:   map[string]map[string]*pb.OrderBook{},
		spreads: map[string]map[string]*pb.Spread{},
		streams: map[string]streamState{},
	}
}

func (s *store) addBook(b *pb.OrderBook, now time.Time) {
	s.received.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.books[b.Symbol]
	if !ok {
		m = map[string]*pb.OrderBook{}
		s.books[b.Symbol] = m
	}
	m[b.Exchange] = b
	s.e2e.add(now.Sub(b.ExchangeTime.AsTime()))
	s.delivery.add(now.Sub(b.PublishedTime.AsTime()))
}

func (s *store) addSpread(sp *pb.Spread) {
	s.received.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.spreads[sp.Symbol]
	if !ok {
		m = map[string]*pb.Spread{}
		s.spreads[sp.Symbol] = m
	}
	m[sp.BuyExchange+"|"+sp.SellExchange] = sp
}

func (s *store) setStream(name string, connected bool, err error) {
	st := streamState{connected: connected}
	if err != nil {
		st.err = status.Convert(err).Message()
	}
	s.mu.Lock()
	s.streams[name] = st
	s.mu.Unlock()
}

// snapshot is a consistent copy of the store for rendering. Protobuf
// messages are never changed after they are received, so sharing the
// pointers is safe.
type snapshot struct {
	symbols    []string
	books      map[string]map[string]*pb.OrderBook
	spreads    map[string]map[string]*pb.Spread
	connected  bool
	lastErr    string
	e2eP50     time.Duration
	e2eP99     time.Duration
	deliverP50 time.Duration
	received   int64
}

func (s *store) snapshot() snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := snapshot{
		books:     make(map[string]map[string]*pb.OrderBook, len(s.books)),
		spreads:   make(map[string]map[string]*pb.Spread, len(s.spreads)),
		connected: len(s.streams) > 0,
		received:  s.received.Load(),
	}
	for sym, m := range s.books {
		snap.symbols = append(snap.symbols, sym)
		cp := make(map[string]*pb.OrderBook, len(m))
		for k, v := range m {
			cp[k] = v
		}
		snap.books[sym] = cp
	}
	for sym, m := range s.spreads {
		cp := make(map[string]*pb.Spread, len(m))
		for k, v := range m {
			cp[k] = v
		}
		snap.spreads[sym] = cp
	}
	for _, st := range s.streams {
		if !st.connected {
			snap.connected = false
			snap.lastErr = st.err
		}
	}
	slices.Sort(snap.symbols)
	snap.e2eP50, snap.e2eP99 = s.e2e.percentile(0.5), s.e2e.percentile(0.99)
	snap.deliverP50 = s.delivery.percentile(0.5)
	return snap
}

// latencyWindow keeps the last N samples.
type latencyWindow struct {
	buf  [2048]time.Duration
	n    int
	next int
}

func (w *latencyWindow) add(d time.Duration) {
	w.buf[w.next] = d
	w.next = (w.next + 1) % len(w.buf)
	w.n = min(w.n+1, len(w.buf))
}

func (w *latencyWindow) percentile(p float64) time.Duration {
	if w.n == 0 {
		return 0
	}
	vals := slices.Clone(w.buf[:w.n])
	slices.Sort(vals)
	return vals[int(float64(len(vals)-1)*p)]
}

// runStream opens a stream with open and reads it with recv until an error,
// then opens it again with backoff. It returns when ctx is done.
func runStream[T any](ctx context.Context, s *store, name string,
	open func(context.Context) (interface{ Recv() (T, error) }, error), handle func(T)) {
	backoff := 500 * time.Millisecond
	for ctx.Err() == nil {
		stream, err := open(ctx)
		if err == nil {
			for {
				var msg T
				msg, err = stream.Recv()
				if err != nil {
					break
				}
				s.setStream(name, true, nil)
				backoff = 500 * time.Millisecond
				handle(msg)
			}
		}
		if ctx.Err() != nil {
			return
		}
		s.setStream(name, false, err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 10*time.Second)
	}
}
