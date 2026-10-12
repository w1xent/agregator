package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/w1xent/agregator/internal/model"
	"github.com/w1xent/agregator/internal/storage"
)

var now = time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)

type fakeHistory struct {
	calls     int
	lastQuery storage.SpreadQuery
	lastQuote storage.QuoteQuery
}

func (f *fakeHistory) SpreadHistory(_ context.Context, q storage.SpreadQuery) ([]storage.SpreadPoint, error) {
	f.calls++
	f.lastQuery = q
	return []storage.SpreadPoint{{Time: q.To, BuyExchange: "bybit", SellExchange: "okx", SpreadBps: 1.2, Samples: 1}}, nil
}

func (f *fakeHistory) QuoteHistory(_ context.Context, q storage.QuoteQuery) ([]storage.QuotePoint, error) {
	f.calls++
	f.lastQuote = q
	return nil, nil
}

type fakeLatest struct {
	err     error
	spreads []model.Spread
}

func (f *fakeLatest) LatestQuotes(context.Context, string) ([]model.Quote, error) {
	if f.err != nil {
		return nil, f.err
	}
	return []model.Quote{{Exchange: "bybit", Symbol: "BTC-USDT", Bid: 1, Ask: 2}}, nil
}

func (f *fakeLatest) LatestSpreads(context.Context, string) ([]model.Spread, error) {
	return f.spreads, f.err
}

type memCache struct {
	mu   sync.Mutex
	data map[string][]byte
}

func (m *memCache) GetJSON(_ context.Context, key string, dst any) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	raw, ok := m.data[key]
	if !ok {
		return false, nil
	}
	return true, json.Unmarshal(raw, dst)
}

func (m *memCache) SetJSON(_ context.Context, key string, v any, _ time.Duration) error {
	raw, err := json.Marshal(v)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = raw
	return err
}

func newTestAPI(h *fakeHistory, latest, fallback Latest) http.Handler {
	return New(Options{
		History:  h,
		Latest:   latest,
		Fallback: fallback,
		Cache:    &memCache{data: map[string][]byte{}},
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:      func() time.Time { return now },
		Checks: map[string]func(context.Context) error{
			"postgres": func(context.Context) error { return nil },
			"redis":    func(context.Context) error { return errors.New("down") },
		},
	})
}

func get(t *testing.T, h http.Handler, url string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, url, nil))
	return rec
}

func TestSpreadHistoryParsesQueryAndUsesCache(t *testing.T) {
	hist := &fakeHistory{}
	api := newTestAPI(hist, &fakeLatest{}, nil)

	url := "/api/v1/spreads/history?symbol=btc/usdt&buy=Bybit&from=-30m&bucket=1m&limit=10"
	rec := get(t, api, url)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	q := hist.lastQuery
	if q.Symbol != "BTC-USDT" || q.BuyExchange != "bybit" || q.SellExchange != "" {
		t.Errorf("bad filters: %+v", q)
	}
	if !q.From.Equal(now.Add(-30*time.Minute)) || !q.To.Equal(now) {
		t.Errorf("bad range: %v .. %v", q.From, q.To)
	}
	if q.Bucket != time.Minute || q.Limit != 10 {
		t.Errorf("bad bucket/limit: %v %d", q.Bucket, q.Limit)
	}
	var body historyResponse[storage.SpreadPoint]
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Count != 1 || body.Bucket != "1m0s" || rec.Header().Get("X-Cache") != "MISS" {
		t.Errorf("bad response: %+v, X-Cache=%s", body, rec.Header().Get("X-Cache"))
	}

	// The same request again is served from the cache.
	rec = get(t, api, url)
	if rec.Header().Get("X-Cache") != "HIT" || hist.calls != 1 {
		t.Errorf("X-Cache=%s, db calls=%d; want HIT and 1", rec.Header().Get("X-Cache"), hist.calls)
	}
}

func TestHistoryValidation(t *testing.T) {
	api := newTestAPI(&fakeHistory{}, &fakeLatest{}, nil)
	bad := []string{
		"/api/v1/spreads/history",                                // no symbol
		"/api/v1/spreads/history?symbol=BTC-USDT&bucket=10ms",    // bucket too small
		"/api/v1/quotes/history?symbol=BTC-USDT&limit=0",         // bad limit
		"/api/v1/quotes/history?symbol=BTC-USDT&from=yesterday",  // bad time
		"/api/v1/quotes/history?symbol=BTC-USDT&from=-1m&to=-2m", // from after to
		"/api/v1/spreads/latest?min_bps=abc",                     // bad number
	}
	for _, url := range bad {
		if rec := get(t, api, url); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", url, rec.Code)
		}
	}
}

func TestQuoteHistoryRFC3339(t *testing.T) {
	hist := &fakeHistory{}
	api := newTestAPI(hist, &fakeLatest{}, nil)
	rec := get(t, api, "/api/v1/quotes/history?symbol=ETH-USDT&exchange=OKX&from=2026-10-12T10:00:00Z&to=2026-10-12T11:00:00Z")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	q := hist.lastQuote
	if q.Exchange != "okx" || q.From.Hour() != 10 || q.To.Hour() != 11 || q.Limit != defaultLimit {
		t.Errorf("bad query %+v", q)
	}
	if body := rec.Body.String(); !json.Valid([]byte(body)) || !strings.Contains(body, `"points":[]`) {
		t.Errorf("empty result must be an empty array: %s", body)
	}
}

func TestLatestSpreadsFilterAndFallback(t *testing.T) {
	spreads := []model.Spread{{Symbol: "BTC-USDT", SpreadBps: 2}, {Symbol: "BTC-USDT", SpreadBps: -3}}
	api := newTestAPI(&fakeHistory{}, &fakeLatest{err: errors.New("redis down")}, &fakeLatest{spreads: spreads})

	rec := get(t, api, "/api/v1/spreads/latest?symbol=BTC-USDT&min_bps=0")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	var body struct {
		Source  string         `json:"source"`
		Spreads []model.Spread `json:"spreads"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Source != "memory" || len(body.Spreads) != 1 || body.Spreads[0].SpreadBps != 2 {
		t.Errorf("bad response %+v", body)
	}
}

func TestReadyz(t *testing.T) {
	api := newTestAPI(&fakeHistory{}, &fakeLatest{}, nil)
	rec := get(t, api, "/readyz")
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), `"redis":"down"`) {
		t.Errorf("status %d body %s", rec.Code, rec.Body)
	}
	if rec := get(t, api, "/healthz"); rec.Code != http.StatusOK {
		t.Errorf("healthz status %d", rec.Code)
	}
}
