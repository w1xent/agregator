package exchange

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/gorilla/websocket"

	"github.com/w1xent/agregator/internal/model"
)

// Options control the connection behaviour of a Stream.
type Options struct {
	// Symbols to subscribe to, in canonical form ("BTC-USDT").
	Symbols []string
	// Depth is the number of levels per side in emitted books (0 = all).
	Depth int
	// PingInterval is how often the application level ping is sent.
	PingInterval time.Duration
	// ReadTimeout closes the connection if no message arrives in this time.
	// It protects against "silent" connections that look open but are dead.
	ReadTimeout time.Duration
	// MinBackoff and MaxBackoff limit the pause between reconnect attempts.
	MinBackoff time.Duration
	MaxBackoff time.Duration
	// Dialer is optional. websocket.DefaultDialer is used when nil.
	Dialer *websocket.Dialer
}

func (o *Options) setDefaults() {
	if o.PingInterval <= 0 {
		o.PingInterval = 20 * time.Second
	}
	if o.ReadTimeout <= 0 {
		o.ReadTimeout = 30 * time.Second
	}
	if o.MinBackoff <= 0 {
		o.MinBackoff = time.Second
	}
	if o.MaxBackoff < o.MinBackoff {
		o.MaxBackoff = max(30*time.Second, o.MinBackoff)
	}
	if o.Dialer == nil {
		o.Dialer = websocket.DefaultDialer
	}
}

// Stream keeps one WebSocket connection to an exchange alive and sends
// normalized books to a channel.
type Stream struct {
	adapter Adapter
	opts    Options
	log     *slog.Logger
}

// NewStream creates a stream for the given adapter.
func NewStream(a Adapter, opts Options, log *slog.Logger) *Stream {
	opts.setDefaults()
	return &Stream{adapter: a, opts: opts, log: log.With("exchange", a.Name())}
}

// Run connects to the exchange and sends books to out until ctx is done.
// Any connection problem leads to a reconnect with exponential backoff.
func (s *Stream) Run(ctx context.Context, out chan<- model.OrderBook) {
	name := s.adapter.Name()
	bo := backoff{min: s.opts.MinBackoff, max: s.opts.MaxBackoff}
	metricConnected.WithLabelValues(name).Set(0)

	for {
		gotData, err := s.session(ctx, out)
		metricConnected.WithLabelValues(name).Set(0)
		if ctx.Err() != nil {
			return
		}
		if gotData {
			// The last connection worked, so start the backoff from scratch.
			bo.reset()
		}
		wait := bo.next()
		metricReconnects.WithLabelValues(name).Inc()
		s.log.Warn("connection lost, reconnecting", "error", err, "retry_in", wait.Round(time.Millisecond).String())

		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// session runs one connection from dial to failure. gotData reports whether
// at least one book was received, which means the connection was healthy.
func (s *Stream) session(ctx context.Context, out chan<- model.OrderBook) (gotData bool, err error) {
	name := s.adapter.Name()

	dialCtx, cancelDial := context.WithTimeout(ctx, 10*time.Second)
	conn, _, err := s.opts.Dialer.DialContext(dialCtx, s.adapter.URL(), nil)
	cancelDial()
	if err != nil {
		metricErrors.WithLabelValues(name, "dial").Inc()
		return false, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	sessCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Closing the connection is the only way to unblock ReadMessage.
	stop := context.AfterFunc(sessCtx, func() { _ = conn.Close() })
	defer stop()

	s.adapter.Reset()

	subs, err := s.adapter.SubscribeMessages(s.opts.Symbols)
	if err != nil {
		return false, fmt.Errorf("build subscribe messages: %w", err)
	}
	for _, msg := range subs {
		if err := s.write(conn, msg); err != nil {
			return false, fmt.Errorf("subscribe: %w", err)
		}
	}
	s.log.Info("connected", "url", s.adapter.URL(), "symbols", s.opts.Symbols)
	metricConnected.WithLabelValues(name).Set(1)

	// From now on the pinger is the only writer, as gorilla/websocket
	// allows one concurrent writer.
	pingErr := make(chan error, 1)
	go func() {
		pingErr <- s.pinger(sessCtx, conn)
		cancel()
	}()

	for {
		_ = conn.SetReadDeadline(time.Now().Add(s.opts.ReadTimeout))
		_, raw, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return gotData, nil
			}
			select {
			case perr := <-pingErr:
				if perr != nil {
					err = perr
				}
			default:
			}
			metricErrors.WithLabelValues(name, "read").Inc()
			return gotData, fmt.Errorf("read: %w", err)
		}

		received := time.Now()
		metricMessages.WithLabelValues(name).Inc()

		books, err := s.adapter.Parse(raw, received)
		if err != nil {
			var exErr *Error
			switch {
			case errors.Is(err, ErrResync):
				metricErrors.WithLabelValues(name, "resync").Inc()
				return gotData, err
			case errors.As(err, &exErr):
				metricErrors.WithLabelValues(name, "exchange").Inc()
				s.log.Warn("exchange reported an error", "code", exErr.Code, "message", exErr.Message)
			default:
				metricErrors.WithLabelValues(name, "parse").Inc()
				s.log.Debug("cannot parse message", "error", err, "raw", truncate(raw, 256))
			}
			continue
		}

		for i := range books {
			b := books[i]
			b.Trim(s.opts.Depth)
			if !b.ExchangeTS.IsZero() {
				metricExchangeLatency.WithLabelValues(name).Observe(received.Sub(b.ExchangeTS).Seconds())
			}
			select {
			case out <- b:
				gotData = true
				metricBooks.WithLabelValues(name).Inc()
			case <-ctx.Done():
				return gotData, nil
			}
		}
	}
}

func (s *Stream) pinger(ctx context.Context, conn *websocket.Conn) error {
	msg := s.adapter.PingMessage()
	if len(msg) == 0 {
		<-ctx.Done()
		return nil
	}
	t := time.NewTicker(s.opts.PingInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := s.write(conn, msg); err != nil {
				return fmt.Errorf("ping: %w", err)
			}
		}
	}
}

func (s *Stream) write(conn *websocket.Conn, msg []byte) error {
	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return conn.WriteMessage(websocket.TextMessage, msg)
}

// backoff returns growing pauses with random jitter, so that many clients do
// not reconnect at exactly the same moment.
type backoff struct {
	min, max, cur time.Duration
}

func (b *backoff) next() time.Duration {
	if b.cur == 0 {
		b.cur = b.min
	}
	d := b.cur
	b.cur = min(b.cur*2, b.max)
	// The result is a random value in range [d/2, d].
	half := d / 2
	return half + rand.N(half+1)
}

func (b *backoff) reset() { b.cur = 0 }

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}
