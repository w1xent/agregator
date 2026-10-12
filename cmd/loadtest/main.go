// Command loadtest measures how the aggregator behaves under load.
//
// It does two things at the same time:
//
//  1. Optional: writes synthetic order books to Kafka at a fixed rate
//     (-rate), like a collector with many exchanges and symbols.
//  2. Opens many gRPC streams (-subscribers) and measures how many
//     messages every client receives and how old they are.
//
// Example:
//
//	go run ./cmd/loadtest -subscribers 100 -rate 5000 -duration 30s
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/w1xent/agregator/gen/marketdata/v1"
	"github.com/w1xent/agregator/internal/kafka"
	"github.com/w1xent/agregator/internal/model"
)

type options struct {
	grpcAddr    string
	metricsURL  string
	subscribers int
	stream      string
	duration    time.Duration
	warmup      time.Duration
	brokers     string
	topic       string
	rate        int
	symbols     int
	exchanges   int
	depth       int
}

func main() {
	var o options
	flag.StringVar(&o.grpcAddr, "grpc", "localhost:50051", "aggregator gRPC address")
	flag.StringVar(&o.metricsURL, "metrics", "http://localhost:8080/metrics", "aggregator metrics URL (to read dropped messages); empty to skip")
	flag.IntVar(&o.subscribers, "subscribers", 50, "number of concurrent gRPC streams")
	flag.StringVar(&o.stream, "stream", "books", "stream to open: books, spreads or both")
	flag.DurationVar(&o.duration, "duration", 30*time.Second, "measurement time")
	flag.DurationVar(&o.warmup, "warmup", 2*time.Second, "time before measurement starts")
	flag.StringVar(&o.brokers, "brokers", "localhost:29092", "Kafka brokers for synthetic data")
	flag.StringVar(&o.topic, "topic", "orderbooks", "Kafka topic")
	flag.IntVar(&o.rate, "rate", 0, "synthetic order books per second written to Kafka (0 = only live data)")
	flag.IntVar(&o.symbols, "symbols", 20, "number of synthetic symbols")
	flag.IntVar(&o.exchanges, "exchanges", 3, "number of synthetic exchanges")
	flag.IntVar(&o.depth, "depth", 10, "levels per side in synthetic books")
	flag.Parse()

	if err := run(o); err != nil {
		fmt.Fprintln(os.Stderr, "loadtest:", err)
		os.Exit(1)
	}
}

// stats are collected by one subscriber goroutine and merged at the end.
type stats struct {
	messages int64
	pipeline *hdrhistogram.Histogram // received_time -> client (µs)
	delivery *hdrhistogram.Histogram // published_time -> client (µs)
}

func newStats() *stats {
	return &stats{
		pipeline: hdrhistogram.New(1, 60_000_000, 3),
		delivery: hdrhistogram.New(1, 60_000_000, 3),
	}
}

func run(o options) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	conn, err := grpc.NewClient(o.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	client := pb.NewMarketDataServiceClient(conn)

	droppedBefore := readDropped(o.metricsURL)

	runCtx, cancel := context.WithTimeout(ctx, o.warmup+o.duration)
	defer cancel()
	measureFrom := time.Now().Add(o.warmup)

	// Producer of synthetic data.
	var produced, produceErrors atomic.Int64
	var wg sync.WaitGroup
	if o.rate > 0 {
		cl, err := kgo.NewClient(
			kgo.SeedBrokers(strings.Split(o.brokers, ",")...),
			kgo.DefaultProduceTopic(o.topic),
			kgo.ProducerLinger(5*time.Millisecond),
			kgo.RequiredAcks(kgo.LeaderAck()),
			kgo.DisableIdempotentWrite(),
			kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		)
		if err != nil {
			return err
		}
		defer cl.Close()
		wg.Go(func() { produce(runCtx, cl, o, &produced, &produceErrors) })
	}

	// Subscribers.
	var streamErrors atomic.Int64
	results := make([]*stats, o.subscribers)
	for i := range o.subscribers {
		results[i] = newStats()
		st := results[i]
		wg.Go(func() {
			if err := subscribe(runCtx, client, o.stream, measureFrom, st); err != nil {
				streamErrors.Add(1)
				fmt.Fprintln(os.Stderr, "stream error:", err)
			}
		})
	}

	fmt.Printf("Running: %d subscribers (%s), synthetic rate %d books/s, warmup %s, duration %s\n",
		o.subscribers, o.stream, o.rate, o.warmup, o.duration)
	wg.Wait()

	// Merge and report.
	total := newStats()
	for _, st := range results {
		total.messages += st.messages
		total.pipeline.Merge(st.pipeline)
		total.delivery.Merge(st.delivery)
	}
	secs := o.duration.Seconds()
	fmt.Println()
	fmt.Println("=== Load test result ===")
	fmt.Printf("subscribers          %d (stream: %s), stream errors: %d\n", o.subscribers, o.stream, streamErrors.Load())
	if o.rate > 0 {
		fmt.Printf("kafka produced       %d books (%.0f/s), errors: %d\n", produced.Load(), float64(produced.Load())/(secs+o.warmup.Seconds()), produceErrors.Load())
	}
	fmt.Printf("messages received    %d total, %.0f msg/s total, %.0f msg/s per subscriber\n",
		total.messages, float64(total.messages)/secs, float64(total.messages)/secs/float64(max(o.subscribers, 1)))
	if droppedBefore >= 0 {
		if after := readDropped(o.metricsURL); after >= 0 {
			fmt.Printf("dropped by server    %.0f messages (slow client protection)\n", after-droppedBefore)
		}
	}
	printLatency("latency received_time -> client (collector/producer to client)", total.pipeline)
	printLatency("latency published_time -> client (gRPC fan-out only)", total.delivery)
	return nil
}

func printLatency(title string, h *hdrhistogram.Histogram) {
	q := func(p float64) string { return fmtUs(h.ValueAtQuantile(p)) }
	fmt.Printf("%s\n    p50 %s   p90 %s   p99 %s   p99.9 %s   max %s\n",
		title, q(50), q(90), q(99), q(99.9), fmtUs(h.Max()))
}

func fmtUs(us int64) string {
	d := time.Duration(us) * time.Microsecond
	if d < time.Millisecond {
		return fmt.Sprintf("%dµs", us)
	}
	return fmt.Sprintf("%.1fms", float64(d)/float64(time.Millisecond))
}

func subscribe(ctx context.Context, client pb.MarketDataServiceClient, kind string, measureFrom time.Time, st *stats) error {
	record := func(received, published time.Time) {
		now := time.Now()
		if now.Before(measureFrom) {
			return
		}
		st.messages++
		_ = st.pipeline.RecordValue(max(now.Sub(received).Microseconds(), 1))
		_ = st.delivery.RecordValue(max(now.Sub(published).Microseconds(), 1))
	}

	var wg sync.WaitGroup
	var firstErr error
	var mu sync.Mutex
	fail := func(err error) {
		if err == nil || ctx.Err() != nil {
			return
		}
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
	}
	// One goroutine per stream; stats are only touched under mu when both
	// streams are used.
	if kind == "books" || kind == "both" {
		wg.Go(func() {
			s, err := client.StreamOrderBooks(ctx, &pb.StreamOrderBooksRequest{})
			if err != nil {
				fail(err)
				return
			}
			for {
				b, err := s.Recv()
				if err != nil {
					fail(err)
					return
				}
				mu.Lock()
				record(b.ReceivedTime.AsTime(), b.PublishedTime.AsTime())
				mu.Unlock()
			}
		})
	}
	if kind == "spreads" || kind == "both" {
		wg.Go(func() {
			s, err := client.StreamSpreads(ctx, &pb.StreamSpreadsRequest{})
			if err != nil {
				fail(err)
				return
			}
			for {
				sp, err := s.Recv()
				if err != nil {
					fail(err)
					return
				}
				mu.Lock()
				// Spreads have no receive time; use the exchange time.
				record(sp.Time.AsTime(), sp.PublishedTime.AsTime())
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	return firstErr
}

// produce writes synthetic books at o.rate per second. Prices follow a
// random walk, so the spread engine gets realistic changing input.
func produce(ctx context.Context, cl *kgo.Client, o options, produced, failed *atomic.Int64) {
	type key struct{ ex, sym int }
	prices := map[key]float64{}
	for e := range o.exchanges {
		for s := range o.symbols {
			prices[key{e, s}] = 100 + float64(s)*10
		}
	}
	exchangeName := func(e int) string { return fmt.Sprintf("lt-ex%d", e+1) }
	symbolName := func(s int) string { return fmt.Sprintf("LT%d-USDT", s+1) }

	const tick = 5 * time.Millisecond
	t := time.NewTicker(tick)
	defer t.Stop()
	start := time.Now()
	sent := 0
	for {
		select {
		case <-ctx.Done():
			_ = cl.Flush(context.Background())
			return
		case now := <-t.C:
			due := int(now.Sub(start).Seconds()*float64(o.rate)) - sent
			for range due {
				k := key{rand.IntN(o.exchanges), rand.IntN(o.symbols)}
				mid := prices[k] * (1 + (rand.Float64()-0.5)*0.0004)
				prices[k] = mid
				b := syntheticBook(exchangeName(k.ex), symbolName(k.sym), mid, o.depth, time.Now())
				val, _ := kafka.Encode(&b)
				// Not ctx: records in flight must not fail when the test ends.
				cl.Produce(context.Background(), &kgo.Record{Key: kafka.Key(&b), Value: val}, func(_ *kgo.Record, err error) {
					if err != nil {
						failed.Add(1)
						return
					}
					produced.Add(1)
				})
				sent++
			}
		}
	}
}

func syntheticBook(exchange, symbol string, mid float64, depth int, now time.Time) model.OrderBook {
	b := model.OrderBook{Exchange: exchange, Symbol: symbol, ExchangeTS: now, ReceivedTS: now}
	step := mid * 0.0001
	for i := range depth {
		b.Bids = append(b.Bids, model.Level{Price: mid - step*float64(i+1), Size: 1 + rand.Float64()*5})
		b.Asks = append(b.Asks, model.Level{Price: mid + step*float64(i+1), Size: 1 + rand.Float64()*5})
	}
	return b
}

// readDropped returns the sum of md_hub_dropped_total from the metrics
// endpoint, or -1 if it is not available.
func readDropped(url string) float64 {
	if url == "" {
		return -1
	}
	c := http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get(url)
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	sum := 0.0
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "md_hub_dropped_total") {
			continue
		}
		fields := strings.Fields(line)
		if v, err := strconv.ParseFloat(fields[len(fields)-1], 64); err == nil {
			sum += v
		}
	}
	return sum
}
