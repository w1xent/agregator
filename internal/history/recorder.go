// Package history samples spreads and quotes and writes them to the
// database in batches.
//
// The live stream has hundreds of updates per second. Saving all of them is
// expensive and not useful for charts, so the recorder keeps at most one row
// per key (symbol + exchange pair, or symbol + exchange) per sample interval.
package history

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/w1xent/agregator/internal/model"
)

var (
	metricRows = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "md_history_rows_written_total",
		Help: "Rows written to the history database.",
	}, []string{"table"})
	metricFlushDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "md_history_flush_seconds",
		Help:    "Duration of one batch write.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 12),
	}, []string{"table"})
	metricErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "md_history_errors_total",
		Help: "Failed batch writes.",
	}, []string{"table"})
	metricDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "md_history_dropped_total",
		Help: "Updates dropped because the recorder queue was full.",
	})
)

// Store writes batches of history rows.
type Store interface {
	InsertSpreads(ctx context.Context, spreads []model.Spread) error
	InsertQuotes(ctx context.Context, quotes []model.Quote) error
}

// Options configure the recorder.
type Options struct {
	SampleInterval time.Duration // default 1s
	FlushInterval  time.Duration // default 1s
	MaxBatch       int           // default 1000
	QueueSize      int           // default 10000
}

// Recorder collects updates and writes them in the background.
type Recorder struct {
	store Store
	opts  Options
	log   *slog.Logger

	spreadsIn chan model.Spread
	quotesIn  chan model.Quote

	// These fields are used only by the Run goroutine.
	lastSpread map[string]time.Time
	lastQuote  map[string]time.Time
	spreads    []model.Spread
	quotes     []model.Quote
}

// New creates a recorder. Call Run to start it.
func New(store Store, opts Options, log *slog.Logger) *Recorder {
	if opts.SampleInterval <= 0 {
		opts.SampleInterval = time.Second
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = time.Second
	}
	if opts.MaxBatch <= 0 {
		opts.MaxBatch = 1000
	}
	if opts.QueueSize <= 0 {
		opts.QueueSize = 10_000
	}
	return &Recorder{
		store:      store,
		opts:       opts,
		log:        log,
		spreadsIn:  make(chan model.Spread, opts.QueueSize),
		quotesIn:   make(chan model.Quote, opts.QueueSize),
		lastSpread: make(map[string]time.Time),
		lastQuote:  make(map[string]time.Time),
	}
}

// AddSpread queues a spread. It never blocks.
func (r *Recorder) AddSpread(s model.Spread) {
	select {
	case r.spreadsIn <- s:
	default:
		metricDropped.Inc()
	}
}

// AddQuote queues a quote. It never blocks.
func (r *Recorder) AddQuote(q model.Quote) {
	select {
	case r.quotesIn <- q:
	default:
		metricDropped.Inc()
	}
}

// Run processes the queue until ctx is done, then writes what is left.
func (r *Recorder) Run(ctx context.Context) {
	ticker := time.NewTicker(r.opts.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// Last flush with a fresh context, the old one is cancelled.
			flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			r.flush(flushCtx)
			cancel()
			return
		case s := <-r.spreadsIn:
			if r.sample(r.lastSpread, s.Key(), s.Time) {
				r.spreads = append(r.spreads, s)
			}
		case q := <-r.quotesIn:
			if r.sample(r.lastQuote, q.Symbol+"|"+q.Exchange, q.ExchangeTS) {
				r.quotes = append(r.quotes, q)
			}
		case <-ticker.C:
			r.flush(ctx)
		}
		if len(r.spreads) >= r.opts.MaxBatch || len(r.quotes) >= r.opts.MaxBatch {
			r.flush(ctx)
		}
	}
}

// sample reports whether the update is the first one for key in its
// interval. Intervals are aligned to the clock, so with 1s sampling we keep
// one row per key per second.
func (r *Recorder) sample(last map[string]time.Time, key string, t time.Time) bool {
	slot := t.Truncate(r.opts.SampleInterval)
	if prev, ok := last[key]; ok && !slot.After(prev) {
		return false
	}
	last[key] = slot
	return true
}

func (r *Recorder) flush(ctx context.Context) {
	if len(r.spreads) > 0 {
		start := time.Now()
		if err := r.store.InsertSpreads(ctx, r.spreads); err != nil {
			metricErrors.WithLabelValues("spreads").Inc()
			r.log.Warn("cannot write spreads", "rows", len(r.spreads), "error", err)
		} else {
			metricRows.WithLabelValues("spreads").Add(float64(len(r.spreads)))
			metricFlushDuration.WithLabelValues("spreads").Observe(time.Since(start).Seconds())
		}
		r.spreads = r.spreads[:0]
	}
	if len(r.quotes) > 0 {
		start := time.Now()
		if err := r.store.InsertQuotes(ctx, r.quotes); err != nil {
			metricErrors.WithLabelValues("quotes").Inc()
			r.log.Warn("cannot write quotes", "rows", len(r.quotes), "error", err)
		} else {
			metricRows.WithLabelValues("quotes").Add(float64(len(r.quotes)))
			metricFlushDuration.WithLabelValues("quotes").Observe(time.Since(start).Seconds())
		}
		r.quotes = r.quotes[:0]
	}
}
