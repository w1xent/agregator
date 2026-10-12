package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/w1xent/agregator/internal/model"
)

// fakeAdapter understands messages like {"seq":1} and {"resync":true}.
type fakeAdapter struct {
	url    string
	resets atomic.Int32
}

func (f *fakeAdapter) Name() string        { return "fake" }
func (f *fakeAdapter) URL() string         { return f.url }
func (f *fakeAdapter) PingMessage() []byte { return []byte("ping") }
func (f *fakeAdapter) Reset()              { f.resets.Add(1) }

func (f *fakeAdapter) SubscribeMessages(symbols []string) ([][]byte, error) {
	return [][]byte{[]byte("subscribe:" + strings.Join(symbols, ","))}, nil
}

func (f *fakeAdapter) Parse(raw []byte, received time.Time) ([]model.OrderBook, error) {
	if string(raw) == "pong" {
		return nil, nil
	}
	var m struct {
		Seq    int64 `json:"seq"`
		Resync bool  `json:"resync"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	if m.Resync {
		return nil, ErrResync
	}
	return []model.OrderBook{{
		Exchange:   "fake",
		Symbol:     "BTC-USDT",
		Bids:       []model.Level{{Price: 1, Size: 1}, {Price: 0.9, Size: 1}},
		Asks:       []model.Level{{Price: 2, Size: 1}, {Price: 2.1, Size: 1}},
		Seq:        m.Seq,
		ReceivedTS: received,
	}}, nil
}

// wsServer runs handler for every new WebSocket connection and counts them.
type wsServer struct {
	*httptest.Server
	conns atomic.Int32
	mu    sync.Mutex
	subs  []string
}

func newWSServer(t *testing.T, handler func(n int32, c *websocket.Conn)) *wsServer {
	t.Helper()
	s := &wsServer{}
	up := websocket.Upgrader{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		n := s.conns.Add(1)
		_, msg, err := c.ReadMessage()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.subs = append(s.subs, string(msg))
		s.mu.Unlock()
		handler(n, c)
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *wsServer) wsURL() string { return "ws" + strings.TrimPrefix(s.URL, "http") }

func drain(c *websocket.Conn) {
	for {
		if _, _, err := c.ReadMessage(); err != nil {
			return
		}
	}
}

func testOptions() Options {
	return Options{
		Symbols:      []string{"BTC-USDT"},
		Depth:        1,
		PingInterval: time.Hour,
		ReadTimeout:  time.Second,
		MinBackoff:   10 * time.Millisecond,
		MaxBackoff:   50 * time.Millisecond,
	}
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func receive(t *testing.T, out <-chan model.OrderBook) model.OrderBook {
	t.Helper()
	select {
	case b := <-out:
		return b
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for a book")
		return model.OrderBook{}
	}
}

func TestStreamReconnectsAfterServerClosesConnection(t *testing.T) {
	srv := newWSServer(t, func(n int32, c *websocket.Conn) {
		_ = c.WriteMessage(websocket.TextMessage, fmt.Appendf(nil, `{"seq":%d}`, n))
		if n == 1 {
			return // drop the first connection right after one message
		}
		drain(c)
	})

	ad := &fakeAdapter{url: srv.wsURL()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan model.OrderBook, 10)
	done := make(chan struct{})
	go func() {
		NewStream(ad, testOptions(), discardLogger()).Run(ctx, out)
		close(done)
	}()

	first := receive(t, out)
	second := receive(t, out)
	if first.Seq != 1 || second.Seq != 2 {
		t.Fatalf("got seq %d and %d, want 1 and 2", first.Seq, second.Seq)
	}
	if len(first.Bids) != 1 || len(first.Asks) != 1 {
		t.Errorf("book was not trimmed to depth 1: %+v", first)
	}
	if got := srv.conns.Load(); got != 2 {
		t.Errorf("connections = %d, want 2", got)
	}
	if got := ad.resets.Load(); got != 2 {
		t.Errorf("adapter resets = %d, want 2 (one per connection)", got)
	}
	srv.mu.Lock()
	for _, s := range srv.subs {
		if s != "subscribe:BTC-USDT" {
			t.Errorf("unexpected subscribe message %q", s)
		}
	}
	srv.mu.Unlock()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after context cancel")
	}
}

func TestStreamReconnectsWhenConnectionIsSilent(t *testing.T) {
	srv := newWSServer(t, func(n int32, c *websocket.Conn) {
		if n == 1 {
			drain(c) // say nothing; the client must give up by read timeout
			return
		}
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"seq":7}`))
		drain(c)
	})

	opts := testOptions()
	opts.ReadTimeout = 150 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan model.OrderBook, 10)
	go NewStream(&fakeAdapter{url: srv.wsURL()}, opts, discardLogger()).Run(ctx, out)

	if b := receive(t, out); b.Seq != 7 {
		t.Fatalf("seq = %d, want 7", b.Seq)
	}
}

func TestStreamReconnectsOnResync(t *testing.T) {
	srv := newWSServer(t, func(n int32, c *websocket.Conn) {
		if n == 1 {
			_ = c.WriteMessage(websocket.TextMessage, []byte(`{"resync":true}`))
		} else {
			_ = c.WriteMessage(websocket.TextMessage, []byte(`{"seq":3}`))
		}
		drain(c)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan model.OrderBook, 10)
	go NewStream(&fakeAdapter{url: srv.wsURL()}, testOptions(), discardLogger()).Run(ctx, out)

	if b := receive(t, out); b.Seq != 3 {
		t.Fatalf("seq = %d, want 3", b.Seq)
	}
	if got := srv.conns.Load(); got != 2 {
		t.Errorf("connections = %d, want 2", got)
	}
}

func TestStreamSendsPings(t *testing.T) {
	pings := make(chan string, 1)
	srv := newWSServer(t, func(_ int32, c *websocket.Conn) {
		_, msg, err := c.ReadMessage()
		if err == nil {
			pings <- string(msg)
		}
		drain(c)
	})

	opts := testOptions()
	opts.PingInterval = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go NewStream(&fakeAdapter{url: srv.wsURL()}, opts, discardLogger()).Run(ctx, make(chan model.OrderBook))

	select {
	case p := <-pings:
		if p != "ping" {
			t.Fatalf("ping message = %q", p)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no ping received")
	}
}

func TestBackoffGrowsAndResets(t *testing.T) {
	b := backoff{min: 100 * time.Millisecond, max: 400 * time.Millisecond}
	limits := []time.Duration{100, 200, 400, 400}
	for i, limit := range limits {
		limit *= time.Millisecond
		d := b.next()
		if d < limit/2 || d > limit {
			t.Errorf("step %d: %v not in [%v, %v]", i, d, limit/2, limit)
		}
	}
	b.reset()
	if d := b.next(); d > 100*time.Millisecond {
		t.Errorf("after reset: %v > min", d)
	}
}
