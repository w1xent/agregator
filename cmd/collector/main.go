// Command collector connects to exchange WebSockets, normalizes order books
// and publishes them to Kafka.
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"

	"github.com/w1xent/agregator/internal/config"
	"github.com/w1xent/agregator/internal/exchange"
	"github.com/w1xent/agregator/internal/exchange/bitget"
	"github.com/w1xent/agregator/internal/exchange/bybit"
	"github.com/w1xent/agregator/internal/exchange/okx"
	"github.com/w1xent/agregator/internal/kafka"
	"github.com/w1xent/agregator/internal/logging"
	"github.com/w1xent/agregator/internal/model"
	"github.com/w1xent/agregator/internal/serve"
)

type cfg struct {
	exchanges       []string
	symbols         []string
	depth           int
	urls            map[string]string
	kafkaBrokers    []string
	kafkaTopic      string
	kafkaPartitions int
	kafkaRetention  time.Duration
	metricsAddr     string
	pingInterval    time.Duration
	readTimeout     time.Duration
	minBackoff      time.Duration
	maxBackoff      time.Duration
	logLevel        string
	logFormat       string
}

func loadConfig() (cfg, error) {
	env := config.New()
	c := cfg{
		exchanges: env.List("EXCHANGES", []string{"bybit", "bitget", "okx"}),
		symbols:   env.List("SYMBOLS", []string{"BTC-USDT", "ETH-USDT", "SOL-USDT"}),
		depth:     env.Int("BOOK_DEPTH", 10),
		urls: map[string]string{
			"bybit":  env.String("BYBIT_WS_URL", ""),
			"bitget": env.String("BITGET_WS_URL", ""),
			"okx":    env.String("OKX_WS_URL", ""),
		},
		kafkaBrokers:    env.List("KAFKA_BROKERS", []string{"localhost:29092"}),
		kafkaTopic:      env.String("KAFKA_TOPIC", "orderbooks"),
		kafkaPartitions: env.Int("KAFKA_PARTITIONS", 6),
		kafkaRetention:  env.Duration("KAFKA_RETENTION", time.Hour),
		metricsAddr:     env.String("METRICS_ADDR", ":9101"),
		// OKX closes idle connections after 30s, so ping more often.
		pingInterval: env.Duration("WS_PING_INTERVAL", 15*time.Second),
		readTimeout:  env.Duration("WS_READ_TIMEOUT", 30*time.Second),
		minBackoff:   env.Duration("WS_MIN_BACKOFF", time.Second),
		maxBackoff:   env.Duration("WS_MAX_BACKOFF", 30*time.Second),
		logLevel:     env.String("LOG_LEVEL", "info"),
		logFormat:    env.String("LOG_FORMAT", "json"),
	}
	for i, s := range c.symbols {
		c.symbols[i] = model.NormalizeSymbol(s)
	}
	return c, env.Err()
}

func newAdapter(name, url string) (exchange.Adapter, error) {
	switch strings.ToLower(name) {
	case "bybit":
		return bybit.New(url), nil
	case "bitget":
		return bitget.New(url), nil
	case "okx":
		return okx.New(url), nil
	default:
		return nil, fmt.Errorf("unknown exchange %q (supported: bybit, bitget, okx)", name)
	}
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "collector:", err)
		os.Exit(1)
	}
}

func run() error {
	c, err := loadConfig()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	log := logging.New("collector", c.logFormat, c.logLevel)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	adapters := make([]exchange.Adapter, 0, len(c.exchanges))
	for _, name := range c.exchanges {
		a, err := newAdapter(name, c.urls[strings.ToLower(name)])
		if err != nil {
			return err
		}
		adapters = append(adapters, a)
	}

	ensureCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	err = kafka.EnsureTopic(ensureCtx, c.kafkaBrokers, c.kafkaTopic, int32(c.kafkaPartitions), c.kafkaRetention, log)
	cancel()
	if err != nil {
		return err
	}

	producer, err := kafka.NewProducer(c.kafkaBrokers, c.kafkaTopic, "md-collector", log)
	if err != nil {
		return err
	}
	defer producer.Close()

	log.Info("collector started", "exchanges", c.exchanges, "symbols", c.symbols, "depth", c.depth, "topic", c.kafkaTopic)

	books := make(chan model.OrderBook, 4096)
	g, gctx := errgroup.WithContext(ctx)

	for _, a := range adapters {
		stream := exchange.NewStream(a, exchange.Options{
			Symbols:      c.symbols,
			Depth:        c.depth,
			PingInterval: c.pingInterval,
			ReadTimeout:  c.readTimeout,
			MinBackoff:   c.minBackoff,
			MaxBackoff:   c.maxBackoff,
		}, log)
		g.Go(func() error {
			stream.Run(gctx, books)
			return nil
		})
	}

	g.Go(func() error {
		for {
			select {
			case <-gctx.Done():
				return nil
			case b := <-books:
				if err := producer.Publish(gctx, &b); err != nil {
					log.Warn("cannot encode book", "error", err)
				}
			}
		}
	})

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	g.Go(func() error {
		return serve.HTTP(gctx, &http.Server{Addr: c.metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second})
	})

	err = g.Wait()
	log.Info("collector stopped")
	return err
}
