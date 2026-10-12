// Package kafka moves normalized order books between the collector and the
// aggregator. Messages are JSON encoded model.OrderBook values. The record
// key is "exchange:symbol", so all updates of one book go to one partition
// and keep their order.
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/w1xent/agregator/internal/model"
)

var (
	metricProduced = promauto.NewCounter(prometheus.CounterOpts{
		Name: "md_kafka_produced_total",
		Help: "Records successfully written to Kafka.",
	})
	metricProduceErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "md_kafka_produce_errors_total",
		Help: "Records that Kafka did not accept.",
	})
	metricProduceDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "md_kafka_produce_dropped_total",
		Help: "Records dropped because the producer buffer was full.",
	})
	metricProduceLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "md_kafka_produce_latency_seconds",
		Help:    "Time from Produce call to broker acknowledgement.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14),
	})
	metricConsumed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "md_kafka_consumed_total",
		Help: "Records read from Kafka.",
	})
	metricConsumeErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "md_kafka_consume_errors_total",
		Help: "Consumer errors by kind (fetch, decode).",
	}, []string{"kind"})
)

// Encode serializes a book for Kafka.
func Encode(b *model.OrderBook) ([]byte, error) { return json.Marshal(b) }

// Decode deserializes a book from Kafka.
func Decode(data []byte) (model.OrderBook, error) {
	var b model.OrderBook
	err := json.Unmarshal(data, &b)
	return b, err
}

// Key returns the record key of a book.
func Key(b *model.OrderBook) []byte { return []byte(b.Exchange + ":" + b.Symbol) }

// EnsureTopic creates the topic if it does not exist. It retries until
// Kafka is reachable or ctx is done.
func EnsureTopic(ctx context.Context, brokers []string, topic string, partitions int32, retention time.Duration, log *slog.Logger) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	configs := map[string]*string{
		"retention.ms": kadm.StringPtr(strconv.FormatInt(retention.Milliseconds(), 10)),
	}

	wait := 500 * time.Millisecond
	for {
		resp, err := adm.CreateTopic(ctx, partitions, -1, configs, topic)
		if err == nil {
			err = resp.Err
		}
		if err == nil || errors.Is(err, kerr.TopicAlreadyExists) {
			return nil
		}
		log.Warn("kafka is not ready, retrying", "error", err, "retry_in", wait.String())
		select {
		case <-ctx.Done():
			return fmt.Errorf("ensure topic %q: %w", topic, err)
		case <-time.After(wait):
		}
		wait = min(wait*2, 5*time.Second)
	}
}

// Producer writes books to Kafka asynchronously.
type Producer struct {
	client  *kgo.Client
	log     *slog.Logger
	lastLog atomic.Int64
}

// NewProducer creates a producer. The settings prefer low latency over
// durability: market data becomes useless after a few seconds anyway.
func NewProducer(brokers []string, topic, clientID string, log *slog.Logger) (*Producer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(clientID),
		kgo.DefaultProduceTopic(topic),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		kgo.RequiredAcks(kgo.LeaderAck()),
		kgo.DisableIdempotentWrite(),
		kgo.RecordDeliveryTimeout(5*time.Second),
		kgo.MaxBufferedRecords(50_000),
	)
	if err != nil {
		return nil, err
	}
	return &Producer{client: cl, log: log}, nil
}

// Publish sends a book without waiting for the broker. If the internal
// buffer is full (Kafka is slow or down) the book is dropped: blocking here
// would stop reading from the exchange WebSocket.
func (p *Producer) Publish(ctx context.Context, b *model.OrderBook) error {
	val, err := Encode(b)
	if err != nil {
		return err
	}
	rec := &kgo.Record{Key: Key(b), Value: val, Timestamp: b.ReceivedTS}
	start := time.Now()
	p.client.TryProduce(ctx, rec, func(_ *kgo.Record, err error) {
		switch {
		case err == nil:
			metricProduced.Inc()
			metricProduceLatency.Observe(time.Since(start).Seconds())
		case errors.Is(err, kgo.ErrMaxBuffered):
			metricProduceDropped.Inc()
			p.logLimited("kafka producer buffer is full, dropping records")
		case errors.Is(err, context.Canceled):
		default:
			metricProduceErrors.Inc()
			p.logLimited("kafka produce failed", "error", err)
		}
	})
	return nil
}

// logLimited writes at most one warning every 5 seconds, so an outage does
// not flood the log.
func (p *Producer) logLimited(msg string, args ...any) {
	now := time.Now().UnixNano()
	last := p.lastLog.Load()
	if now-last < int64(5*time.Second) || !p.lastLog.CompareAndSwap(last, now) {
		return
	}
	p.log.Warn(msg, args...)
}

// Close flushes buffered records (waiting at most 5 seconds) and closes
// the client.
func (p *Producer) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = p.client.Flush(ctx)
	p.client.Close()
}

// Consumer reads books from Kafka.
//
// It does not use a consumer group: every aggregator instance reads all
// partitions from the newest offset. Old market data has no value after a
// restart, and every instance needs the full market state to compute spreads.
type Consumer struct {
	client *kgo.Client
	log    *slog.Logger
}

// NewConsumer creates a consumer that starts at the end of the topic.
func NewConsumer(brokers []string, topic, clientID string, log *slog.Logger) (*Consumer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(clientID),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	if err != nil {
		return nil, err
	}
	return &Consumer{client: cl, log: log}, nil
}

// Run calls handle for every book until ctx is done. handle is called from
// one goroutine, so it sees books in partition order.
func (c *Consumer) Run(ctx context.Context, handle func(model.OrderBook)) {
	for {
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			metricConsumeErrors.WithLabelValues("fetch").Inc()
			c.log.Warn("kafka fetch error", "topic", topic, "partition", partition, "error", err)
		})
		fetches.EachRecord(func(r *kgo.Record) {
			metricConsumed.Inc()
			b, err := Decode(r.Value)
			if err != nil {
				metricConsumeErrors.WithLabelValues("decode").Inc()
				return
			}
			handle(b)
		})
	}
}

// Close closes the client.
func (c *Consumer) Close() { c.client.Close() }
