// Package httpapi is the REST API of the aggregator: latest prices and
// spreads, history from PostgreSQL, health checks and Prometheus metrics.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/w1xent/agregator/internal/model"
	"github.com/w1xent/agregator/internal/storage"
)

var (
	metricRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "md_http_requests_total",
		Help: "HTTP requests by route and status code.",
	}, []string{"route", "code"})
	metricDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "md_http_request_duration_seconds",
		Help:    "HTTP request duration by route.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14),
	}, []string{"route"})
)

// History reads the history from the database.
type History interface {
	SpreadHistory(ctx context.Context, q storage.SpreadQuery) ([]storage.SpreadPoint, error)
	QuoteHistory(ctx context.Context, q storage.QuoteQuery) ([]storage.QuotePoint, error)
}

// Latest returns the latest market state.
type Latest interface {
	LatestQuotes(ctx context.Context, symbol string) ([]model.Quote, error)
	LatestSpreads(ctx context.Context, symbol string) ([]model.Spread, error)
}

// ResponseCache stores JSON responses for a short time.
type ResponseCache interface {
	GetJSON(ctx context.Context, key string, dst any) (bool, error)
	SetJSON(ctx context.Context, key string, v any, ttl time.Duration) error
}

// Options configure the handler.
type Options struct {
	History History
	// Latest is the primary source of latest data (Redis).
	Latest Latest
	// Fallback is used when Latest fails (in-memory state of the service).
	Fallback Latest
	// Cache is optional. History responses are cached for CacheTTL.
	Cache    ResponseCache
	CacheTTL time.Duration
	// Checks are run by /readyz, for example "postgres" -> Ping.
	Checks map[string]func(context.Context) error
	Log    *slog.Logger
	// Now is used in tests. Default time.Now.
	Now func() time.Time
}

const (
	defaultLimit  = 500
	maxLimit      = 10_000
	defaultPeriod = time.Hour
)

type api struct{ Options }

// New returns the HTTP handler with all routes.
func New(opts Options) http.Handler {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.CacheTTL <= 0 {
		opts.CacheTTL = 3 * time.Second
	}
	a := &api{opts}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/quotes/latest", a.latestQuotes)
	mux.HandleFunc("GET /api/v1/spreads/latest", a.latestSpreads)
	mux.HandleFunc("GET /api/v1/quotes/history", a.quoteHistory)
	mux.HandleFunc("GET /api/v1/spreads/history", a.spreadHistory)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", a.ready)
	mux.Handle("GET /metrics", promhttp.Handler())
	return a.middleware(mux)
}

func (a *api) latestQuotes(w http.ResponseWriter, r *http.Request) {
	symbol := model.NormalizeSymbol(r.URL.Query().Get("symbol"))
	source := "redis"
	quotes, err := a.Latest.LatestQuotes(r.Context(), symbol)
	if err != nil && a.Fallback != nil {
		a.Log.Warn("redis is not available, using in-memory state", "error", err)
		source = "memory"
		quotes, err = a.Fallback.LatestQuotes(r.Context(), symbol)
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"source": source, "count": len(quotes), "quotes": quotes})
}

func (a *api) latestSpreads(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	symbol := model.NormalizeSymbol(q.Get("symbol"))
	var minBps *float64
	if v := q.Get("min_bps"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("min_bps: %q is not a number", v))
			return
		}
		minBps = &f
	}

	source := "redis"
	spreads, err := a.Latest.LatestSpreads(r.Context(), symbol)
	if err != nil && a.Fallback != nil {
		a.Log.Warn("redis is not available, using in-memory state", "error", err)
		source = "memory"
		spreads, err = a.Fallback.LatestSpreads(r.Context(), symbol)
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if minBps != nil {
		filtered := spreads[:0]
		for _, s := range spreads {
			if s.SpreadBps >= *minBps {
				filtered = append(filtered, s)
			}
		}
		spreads = filtered
	}
	writeJSON(w, http.StatusOK, map[string]any{"source": source, "count": len(spreads), "spreads": spreads})
}

// historyResponse is the body of both history endpoints.
type historyResponse[T any] struct {
	Symbol string    `json:"symbol"`
	From   time.Time `json:"from"`
	To     time.Time `json:"to"`
	Bucket string    `json:"bucket,omitempty"`
	Count  int       `json:"count"`
	Points []T       `json:"points"`
}

func (a *api) spreadHistory(w http.ResponseWriter, r *http.Request) {
	p, err := a.parseHistory(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	q := r.URL.Query()
	query := storage.SpreadQuery{
		Symbol:       p.symbol,
		BuyExchange:  strings.ToLower(q.Get("buy")),
		SellExchange: strings.ToLower(q.Get("sell")),
		From:         p.from, To: p.to, Bucket: p.bucket, Limit: p.limit,
	}
	cached(a, w, r, func(ctx context.Context) (historyResponse[storage.SpreadPoint], error) {
		points, err := a.History.SpreadHistory(ctx, query)
		return newHistoryResponse(p, points), err
	})
}

func (a *api) quoteHistory(w http.ResponseWriter, r *http.Request) {
	p, err := a.parseHistory(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	query := storage.QuoteQuery{
		Symbol:   p.symbol,
		Exchange: strings.ToLower(r.URL.Query().Get("exchange")),
		From:     p.from, To: p.to, Bucket: p.bucket, Limit: p.limit,
	}
	cached(a, w, r, func(ctx context.Context) (historyResponse[storage.QuotePoint], error) {
		points, err := a.History.QuoteHistory(ctx, query)
		return newHistoryResponse(p, points), err
	})
}

func newHistoryResponse[T any](p historyParams, points []T) historyResponse[T] {
	if points == nil {
		points = []T{}
	}
	resp := historyResponse[T]{Symbol: p.symbol, From: p.from, To: p.to, Count: len(points), Points: points}
	if p.bucket > 0 {
		resp.Bucket = p.bucket.String()
	}
	return resp
}

// cached serves a response from the cache, or builds it with load and
// stores it. Cache errors are not fatal: the request goes to the database.
func cached[T any](a *api, w http.ResponseWriter, r *http.Request, load func(context.Context) (T, error)) {
	ctx := r.Context()
	key := "md:http:" + r.URL.Path + "?" + r.URL.Query().Encode()
	var resp T
	if a.Cache != nil {
		hit, err := a.Cache.GetJSON(ctx, key, &resp)
		if err != nil {
			a.Log.Debug("response cache read failed", "error", err)
		}
		if hit && err == nil {
			w.Header().Set("X-Cache", "HIT")
			writeJSON(w, http.StatusOK, resp)
			return
		}
	}

	resp, err := load(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if a.Cache != nil {
		if err := a.Cache.SetJSON(ctx, key, resp, a.CacheTTL); err != nil {
			a.Log.Debug("response cache write failed", "error", err)
		}
		w.Header().Set("X-Cache", "MISS")
	}
	writeJSON(w, http.StatusOK, resp)
}

type historyParams struct {
	symbol   string
	from, to time.Time
	bucket   time.Duration
	limit    int
}

func (a *api) parseHistory(r *http.Request) (historyParams, error) {
	q := r.URL.Query()
	now := a.Now().UTC()
	p := historyParams{symbol: model.NormalizeSymbol(q.Get("symbol")), to: now, limit: defaultLimit}
	if p.symbol == "" {
		return p, errors.New("symbol is required, for example symbol=BTC-USDT")
	}

	var err error
	if v := q.Get("to"); v != "" {
		if p.to, err = parseTime(v, now); err != nil {
			return p, fmt.Errorf("to: %w", err)
		}
	}
	p.from = p.to.Add(-defaultPeriod)
	if v := q.Get("from"); v != "" {
		if p.from, err = parseTime(v, now); err != nil {
			return p, fmt.Errorf("from: %w", err)
		}
	}
	if !p.from.Before(p.to) {
		return p, errors.New("from must be before to")
	}

	if v := q.Get("bucket"); v != "" {
		p.bucket, err = time.ParseDuration(v)
		if err != nil || p.bucket < time.Second || p.bucket > 24*time.Hour {
			return p, fmt.Errorf("bucket: %q must be a duration between 1s and 24h, for example 1m", v)
		}
	}
	if v := q.Get("limit"); v != "" {
		p.limit, err = strconv.Atoi(v)
		if err != nil || p.limit < 1 || p.limit > maxLimit {
			return p, fmt.Errorf("limit: %q must be a number from 1 to %d", v, maxLimit)
		}
	}
	return p, nil
}

// parseTime accepts RFC 3339 ("2026-10-12T10:00:00Z") or a duration that
// means "ago" ("15m" or "-15m").
func parseTime(v string, now time.Time) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, v); err == nil {
		return t.UTC(), nil
	}
	d, err := time.ParseDuration(strings.TrimPrefix(v, "-"))
	if err != nil {
		return time.Time{}, fmt.Errorf("%q is not RFC 3339 time or a duration like 15m", v)
	}
	return now.Add(-d), nil
}

func (a *api) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	status := http.StatusOK
	result := map[string]string{}
	for name, check := range a.Checks {
		if err := check(ctx); err != nil {
			status = http.StatusServiceUnavailable
			result[name] = err.Error()
		} else {
			result[name] = "ok"
		}
	}
	writeJSON(w, status, result)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// middleware records metrics, logs requests and recovers from panics.
func (a *api) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		defer func() {
			if rec := recover(); rec != nil {
				a.Log.Error("panic in handler", "panic", rec, "path", r.URL.Path)
				writeError(sw, http.StatusInternalServerError, errors.New("internal error"))
			}
			// r.Pattern is set by ServeMux; it keeps the metric cardinality low.
			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}
			metricRequests.WithLabelValues(route, strconv.Itoa(sw.status)).Inc()
			metricDuration.WithLabelValues(route).Observe(time.Since(start).Seconds())
			if r.URL.Path != "/metrics" {
				a.Log.Debug("http request", "method", r.Method, "path", r.URL.Path, "status", sw.status, "duration", time.Since(start))
			}
		}()
		next.ServeHTTP(sw, r)
	})
}

// StateLatest serves the latest data from the in-memory state of the
// aggregator. It is the fallback when Redis is not available.
type StateLatest struct {
	State interface {
		Quotes() []model.Quote
		Spreads() []model.Spread
	}
}

func (s StateLatest) LatestQuotes(_ context.Context, symbol string) ([]model.Quote, error) {
	var out []model.Quote
	for _, q := range s.State.Quotes() {
		if symbol == "" || q.Symbol == symbol {
			out = append(out, q)
		}
	}
	return out, nil
}

func (s StateLatest) LatestSpreads(_ context.Context, symbol string) ([]model.Spread, error) {
	var out []model.Spread
	for _, sp := range s.State.Spreads() {
		if symbol == "" || sp.Symbol == symbol {
			out = append(out, sp)
		}
	}
	return out, nil
}
