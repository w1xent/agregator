// Command aggregator reads order books from Kafka, computes cross-exchange
// spreads and serves them over gRPC streams and a REST API.
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	pb "github.com/w1xent/agregator/gen/marketdata/v1"
	"github.com/w1xent/agregator/internal/aggregator"
	"github.com/w1xent/agregator/internal/cache"
	"github.com/w1xent/agregator/internal/config"
	"github.com/w1xent/agregator/internal/grpcapi"
	"github.com/w1xent/agregator/internal/history"
	"github.com/w1xent/agregator/internal/httpapi"
	"github.com/w1xent/agregator/internal/hub"
	"github.com/w1xent/agregator/internal/kafka"
	"github.com/w1xent/agregator/internal/logging"
	"github.com/w1xent/agregator/internal/serve"
	"github.com/w1xent/agregator/internal/spread"
	"github.com/w1xent/agregator/internal/storage"
)

type cfg struct {
	kafkaBrokers     []string
	kafkaTopic       string
	kafkaPartitions  int
	kafkaRetention   time.Duration
	postgresDSN      string
	redisAddr        string
	redisPassword    string
	redisDB          int
	grpcAddr         string
	httpAddr         string
	staleAfter       time.Duration
	streamBuffer     int
	snapshotInterval time.Duration
	sampleInterval   time.Duration
	flushInterval    time.Duration
	historyRetention time.Duration
	httpCacheTTL     time.Duration
	logLevel         string
	logFormat        string
}

func loadConfig() (cfg, error) {
	env := config.New()
	c := cfg{
		kafkaBrokers:     env.List("KAFKA_BROKERS", []string{"localhost:29092"}),
		kafkaTopic:       env.String("KAFKA_TOPIC", "orderbooks"),
		kafkaPartitions:  env.Int("KAFKA_PARTITIONS", 6),
		kafkaRetention:   env.Duration("KAFKA_RETENTION", time.Hour),
		postgresDSN:      env.String("POSTGRES_DSN", "postgres://md:md@localhost:5432/marketdata?sslmode=disable"),
		redisAddr:        env.String("REDIS_ADDR", "localhost:6379"),
		redisPassword:    env.String("REDIS_PASSWORD", ""),
		redisDB:          env.Int("REDIS_DB", 0),
		grpcAddr:         env.String("GRPC_ADDR", ":50051"),
		httpAddr:         env.String("HTTP_ADDR", ":8080"),
		staleAfter:       env.Duration("STALE_AFTER", 10*time.Second),
		streamBuffer:     env.Int("STREAM_BUFFER", 1024),
		snapshotInterval: env.Duration("SNAPSHOT_INTERVAL", 500*time.Millisecond),
		sampleInterval:   env.Duration("HISTORY_SAMPLE_INTERVAL", time.Second),
		flushInterval:    env.Duration("HISTORY_FLUSH_INTERVAL", time.Second),
		historyRetention: env.Duration("HISTORY_RETENTION", 7*24*time.Hour),
		httpCacheTTL:     env.Duration("HTTP_CACHE_TTL", 3*time.Second),
		logLevel:         env.String("LOG_LEVEL", "info"),
		logFormat:        env.String("LOG_FORMAT", "json"),
	}
	return c, env.Err()
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "aggregator:", err)
		os.Exit(1)
	}
}

func run() error {
	c, err := loadConfig()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	log := logging.New("aggregator", c.logFormat, c.logLevel)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Dependencies. Each of them waits until the service is reachable.
	startCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	pg, err := storage.Connect(startCtx, c.postgresDSN, log)
	if err != nil {
		return err
	}
	defer pg.Close()
	if err := pg.Migrate(startCtx); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	// Redis is only a cache: the service works without it.
	rdb := cache.NewRedis(c.redisAddr, c.redisPassword, c.redisDB, 10*c.snapshotInterval+5*time.Second)
	defer rdb.Close()
	if err := rdb.Ping(startCtx); err != nil {
		log.Warn("redis is not available, latest data will be served from memory", "error", err)
	}

	if err := kafka.EnsureTopic(startCtx, c.kafkaBrokers, c.kafkaTopic, int32(c.kafkaPartitions), c.kafkaRetention, log); err != nil {
		return err
	}
	consumer, err := kafka.NewConsumer(c.kafkaBrokers, c.kafkaTopic, "md-aggregator", log)
	if err != nil {
		return err
	}
	defer consumer.Close()

	// Pipeline.
	engine := spread.NewEngine(c.staleAfter)
	booksHub := hub.New[*pb.OrderBook](c.streamBuffer, aggregator.MetricHubDropped.WithLabelValues("books").Inc)
	spreadsHub := hub.New[*pb.Spread](c.streamBuffer, aggregator.MetricHubDropped.WithLabelValues("spreads").Inc)
	recorder := history.New(pg, history.Options{SampleInterval: c.sampleInterval, FlushInterval: c.flushInterval}, log)
	svc := &aggregator.Service{
		Engine:     engine,
		Books:      booksHub,
		Spreads:    spreadsHub,
		Recorder:   recorder,
		StaleAfter: c.staleAfter,
		Log:        log,
	}

	// gRPC.
	api := grpcapi.NewServer(engine, booksHub, spreadsHub)
	grpcServer := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: 30 * time.Second, Timeout: 10 * time.Second}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true}),
	)
	pb.RegisterMarketDataServiceServer(grpcServer, api)
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthSrv)
	reflection.Register(grpcServer) // lets grpcurl discover the API

	// REST.
	httpHandler := httpapi.New(httpapi.Options{
		History:  pg,
		Latest:   rdb,
		Fallback: httpapi.StateLatest{State: engine},
		Cache:    rdb,
		CacheTTL: c.httpCacheTTL,
		Checks: map[string]func(context.Context) error{
			"postgres": pg.Ping,
			"redis":    rdb.Ping,
		},
		Log: log,
	})

	log.Info("aggregator started", "grpc", c.grpcAddr, "http", c.httpAddr, "topic", c.kafkaTopic)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		consumer.Run(gctx, svc.Process)
		return nil
	})
	g.Go(func() error {
		recorder.Run(gctx)
		return nil
	})
	g.Go(func() error {
		svc.RunSnapshots(gctx, rdb, c.snapshotInterval)
		return nil
	})
	g.Go(func() error {
		svc.RunRetention(gctx, pg, c.historyRetention)
		return nil
	})
	g.Go(func() error {
		return serve.GRPC(gctx, grpcServer, c.grpcAddr, func() {
			healthSrv.Shutdown()
			api.Shutdown()
		})
	})
	g.Go(func() error {
		return serve.HTTP(gctx, &http.Server{Addr: c.httpAddr, Handler: httpHandler, ReadHeaderTimeout: 5 * time.Second})
	})

	err = g.Wait()
	log.Info("aggregator stopped")
	return err
}
