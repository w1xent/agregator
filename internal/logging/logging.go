// Package logging creates the structured logger used by all services.
package logging

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

// New returns a slog logger. format is "json" or "text", level is one of
// "debug", "info", "warn", "error".
func New(service, format, level string) *slog.Logger {
	return NewWithWriter(os.Stdout, service, format, level)
}

// NewWithWriter is like New but writes to w.
func NewWithWriter(w io.Writer, service, format, level string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: parseLevel(level)}
	var h slog.Handler
	if strings.EqualFold(format, "text") {
		h = slog.NewTextHandler(w, opts)
	} else {
		h = slog.NewJSONHandler(w, opts)
	}
	return slog.New(h).With("service", service)
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
