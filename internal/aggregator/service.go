// Package aggregator is the main pipeline of the aggregator service:
//
//	Kafka -> Process -> spread engine -> gRPC hubs
//	                                  -> history recorder (PostgreSQL)
//	        RunSnapshots -> Redis (latest state) + Prometheus gauges
package aggregator

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/w1xent/agregator/gen/marketdata/v1"
	"github.com/w1xent/agregator/internal/grpcapi"
	"github.com/w1xent/agregator/internal/hub"
	"github.com/w1xent/agregator/internal/model"
	"github.com/w1xent/agregator/internal/spread"
)

var latencyBuckets = prometheus.ExponentialBuckets(0.001, 2, 14) // 1ms .. ~8s

var (
	metricBooks = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "md_aggregator_books_total",
		Help: "Order books processed by the aggregator.",
	}, []string{"exchange"})
	metricStale = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "md_aggregator_stale_total",
		Help: "Order books skipped because they were too old on arrival.",
	}, []string{"exchange"})
	metricPipelineLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "md_pipeline_latency_seconds",
		Help:    "Time from collector receive to aggregator processing (mostly Kafka).",
		Buckets: latencyBuckets,
	}, []string{"exchange"})
	metricEndToEnd = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "md_end_to_end_latency_seconds",
		Help:    "Time from the exchange event to aggregator processing.",
		Buckets: latencyBuckets,
	}, []string{"exchange"})
	metricSpreadBps = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "md_spread_bps",
		Help: "Current cross-exchange spread in basis points.",
	}, []string{"symbol", "buy", "sell"})
	metricSpreadsComputed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "md_spreads_computed_total",
		Help: "Spread updates produced by the engine.",
	})
	// MetricHubDropped counts messages dropped for slow gRPC clients.
	MetricHubDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "md_hub_dropped_total",
		Help: "Messages dropped because a gRPC client was too slow.",
	}, []string{"stream"})
)

// Recorder saves history.
type Recorder interface {
	AddSpread(model.Spread)
	AddQuote(model.Quote)
}

// LatestStore keeps the latest market state (Redis).
type LatestStore interface {
	StoreLatest(ctx context.Context, quotes []model.Quote, spreads []model.Spread) error
}

// Service processes books from Kafka.
type Service struct {
	Engine     *spread.Engine
	Books      *hub.Hub[*pb.OrderBook]
	Spreads    *hub.Hub[*pb.Spread]
	Recorder   Recorder
	StaleAfter time.Duration
	Log        *slog.Logger

	now func() time.Time
}

// Process handles one book. It is called from a single goroutine.
func (s *Service) Process(b model.OrderBook) {
	now := time.Now()
	if s.now != nil {
		now = s.now()
	}

	age := now.Sub(b.ReceivedTS)
	if s.StaleAfter > 0 && age > s.StaleAfter {
		// For example old records after a restart or a long Kafka outage.
		metricStale.WithLabelValues(b.Exchange).Inc()
		return
	}
	metricBooks.WithLabelValues(b.Exchange).Inc()
	metricPipelineLatency.WithLabelValues(b.Exchange).Observe(age.Seconds())
	if !b.ExchangeTS.IsZero() {
		metricEndToEnd.WithLabelValues(b.Exchange).Observe(now.Sub(b.ExchangeTS).Seconds())
	}

	spreads := s.Engine.Update(b)

	published := timestamppb.New(now)
	s.Books.Publish(grpcapi.ToProtoBook(&b, published))
	for i := range spreads {
		s.Spreads.Publish(grpcapi.ToProtoSpread(&spreads[i], published))
		s.Recorder.AddSpread(spreads[i])
	}
	metricSpreadsComputed.Add(float64(len(spreads)))
	if q, ok := model.QuoteOf(&b); ok {
		s.Recorder.AddQuote(q)
	}
}

// RunSnapshots writes the latest state to the store and updates the spread
// gauges every interval until ctx is done.
func (s *Service) RunSnapshots(ctx context.Context, store LatestStore, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	gauges := map[[3]string]bool{}
	failing := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		quotes, spreads := s.Engine.Quotes(), s.Engine.Spreads()
		gauges = updateGauges(gauges, spreads)

		if store == nil {
			continue
		}
		wctx, cancel := context.WithTimeout(ctx, interval)
		err := store.StoreLatest(wctx, quotes, spreads)
		cancel()
		switch {
		case err != nil && !failing:
			failing = true
			s.Log.Warn("cannot write latest state to redis", "error", err)
		case err == nil && failing:
			failing = false
			s.Log.Info("redis is available again")
		}
	}
}

// updateGauges sets md_spread_bps for current spreads and removes series of
// pairs that are gone (for example a disconnected exchange).
func updateGauges(prev map[[3]string]bool, spreads []model.Spread) map[[3]string]bool {
	cur := make(map[[3]string]bool, len(spreads))
	for _, sp := range spreads {
		key := [3]string{sp.Symbol, sp.BuyExchange, sp.SellExchange}
		cur[key] = true
		metricSpreadBps.WithLabelValues(key[:]...).Set(sp.SpreadBps)
	}
	for key := range prev {
		if !cur[key] {
			metricSpreadBps.DeleteLabelValues(key[:]...)
		}
	}
	return cur
}

// Cleaner removes old history.
type Cleaner interface {
	DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error)
}

// RunRetention deletes history older than retention every hour.
func (s *Service) RunRetention(ctx context.Context, c Cleaner, retention time.Duration) {
	if retention <= 0 {
		return
	}
	clean := func() {
		n, err := c.DeleteOlderThan(ctx, time.Now().Add(-retention))
		if err != nil {
			if ctx.Err() == nil {
				s.Log.Warn("history cleanup failed", "error", err)
			}
			return
		}
		if n > 0 {
			s.Log.Info("old history removed", "rows", n, "retention", retention)
		}
	}
	clean()
	t := time.NewTicker(time.Hour)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			clean()
		}
	}
}
