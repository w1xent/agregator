package exchange

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	metricMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "md_ws_messages_total",
		Help: "Raw WebSocket messages received from exchanges.",
	}, []string{"exchange"})

	metricBooks = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "md_books_emitted_total",
		Help: "Normalized order books produced by exchange connectors.",
	}, []string{"exchange"})

	metricErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "md_ws_errors_total",
		Help: "Connector errors by kind (dial, read, parse, exchange, resync).",
	}, []string{"exchange", "kind"})

	metricReconnects = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "md_ws_reconnects_total",
		Help: "How many times a connector had to reconnect.",
	}, []string{"exchange"})

	metricConnected = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "md_ws_connected",
		Help: "1 if the connector is connected and subscribed, 0 otherwise.",
	}, []string{"exchange"})

	metricExchangeLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "md_exchange_latency_seconds",
		Help:    "Time between the exchange event time and the moment the collector received it.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14), // 1ms .. ~8s
	}, []string{"exchange"})
)
