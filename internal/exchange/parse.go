package exchange

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/w1xent/agregator/internal/model"
)

// ParseLevels converts exchange levels like [["83490.6","2.41"], ...] to
// model levels. Extra fields after price and size are ignored (OKX sends 4).
func ParseLevels(raw [][]string) ([]model.Level, error) {
	out := make([]model.Level, 0, len(raw))
	for _, l := range raw {
		if len(l) < 2 {
			return nil, fmt.Errorf("bad level %v", l)
		}
		p, err := strconv.ParseFloat(l[0], 64)
		if err != nil {
			return nil, fmt.Errorf("bad price %q: %w", l[0], err)
		}
		s, err := strconv.ParseFloat(l[1], 64)
		if err != nil {
			return nil, fmt.Errorf("bad size %q: %w", l[1], err)
		}
		out = append(out, model.Level{Price: p, Size: s})
	}
	return out, nil
}

// ParseMillis converts a unix time in milliseconds (as a string) to time.Time.
func ParseMillis(s string) (time.Time, error) {
	ms, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("bad timestamp %q: %w", s, err)
	}
	return time.UnixMilli(ms), nil
}

// CodeString returns an error code that may be sent as a JSON number or string.
func CodeString(raw json.RawMessage) string {
	return strings.Trim(string(raw), `"`)
}

// Chunk splits items into groups of at most n items.
func Chunk[T any](items []T, n int) [][]T {
	var out [][]T
	for n < len(items) {
		items, out = items[n:], append(out, items[:n:n])
	}
	if len(items) > 0 {
		out = append(out, items)
	}
	return out
}
