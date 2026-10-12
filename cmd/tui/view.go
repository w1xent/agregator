package main

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	pb "github.com/w1xent/agregator/gen/marketdata/v1"
)

var (
	colGreen  = lipgloss.Color("#22c55e")
	colRed    = lipgloss.Color("#ef4444")
	colYellow = lipgloss.Color("#eab308")
	colDim    = lipgloss.Color("#6b7280")
	colAccent = lipgloss.Color("#38bdf8")

	stTitle   = lipgloss.NewStyle().Bold(true).Foreground(colAccent)
	stGreen   = lipgloss.NewStyle().Foreground(colGreen)
	stRed     = lipgloss.NewStyle().Foreground(colRed)
	stYellow  = lipgloss.NewStyle().Foreground(colYellow)
	stDim     = lipgloss.NewStyle().Foreground(colDim)
	stBold    = lipgloss.NewStyle().Bold(true)
	stBest    = lipgloss.NewStyle().Foreground(colGreen).Bold(true)
	stTab     = lipgloss.NewStyle().Padding(0, 1).Foreground(colDim)
	stTabSel  = lipgloss.NewStyle().Padding(0, 1).Bold(true).Foreground(lipgloss.Color("#0b1220")).Background(colAccent)
	stSection = lipgloss.NewStyle().Bold(true).Foreground(colAccent).MarginTop(1)
	stBox     = lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(colDim).Padding(0, 1)
)

// staleAfter marks data as old in the UI.
const staleAfter = 5 * time.Second

type tickMsg time.Time

func tick() tea.Cmd {
	return tea.Tick(200*time.Millisecond, func(t time.Time) tea.Msg { return tickMsg(t) })
}

type model struct {
	st    *store
	addr  string
	depth int

	selected string
	snap     snapshot
	width    int

	lastCount int64
	lastTick  time.Time
	rate      float64
}

func (m model) Init() tea.Cmd { return tick() }

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c", "esc":
			return m, tea.Quit
		case "right", "l", "tab":
			m.moveSelection(1)
		case "left", "h", "shift+tab":
			m.moveSelection(-1)
		default:
			if n, err := strconv.Atoi(msg.String()); err == nil && n >= 1 && n <= len(m.snap.symbols) {
				m.selected = m.snap.symbols[n-1]
			}
		}
	case tea.WindowSizeMsg:
		m.width = msg.Width
	case tickMsg:
		now := time.Time(msg)
		m.snap = m.st.snapshot()
		if !m.lastTick.IsZero() {
			if dt := now.Sub(m.lastTick).Seconds(); dt > 0 {
				// Smooth the rate a little so the number does not jump.
				cur := float64(m.snap.received-m.lastCount) / dt
				m.rate = 0.7*m.rate + 0.3*cur
			}
		}
		m.lastCount, m.lastTick = m.snap.received, now
		if !slices.Contains(m.snap.symbols, m.selected) && len(m.snap.symbols) > 0 {
			m.selected = m.snap.symbols[0]
		}
		return m, tick()
	}
	return m, nil
}

func (m *model) moveSelection(step int) {
	syms := m.snap.symbols
	if len(syms) == 0 {
		return
	}
	i := slices.Index(syms, m.selected)
	m.selected = syms[(i+step+len(syms))%len(syms)]
}

func (m model) View() string {
	var b strings.Builder
	b.WriteString(m.header())
	b.WriteString("\n")
	b.WriteString(m.tabs())
	b.WriteString("\n")

	if m.selected == "" {
		b.WriteString("\n" + stDim.Render("  Waiting for data from the aggregator...") + "\n")
		return b.String()
	}
	books := m.snap.books[m.selected]
	exchanges := sortedKeys(books)
	prec := pricePrecision(books)

	b.WriteString(stSection.Render("Best prices") + "\n")
	b.WriteString(m.quotesTable(books, exchanges, prec))
	b.WriteString(stSection.Render(fmt.Sprintf("Order books (top %d)", m.depth)) + "\n")
	b.WriteString(m.orderBooks(books, exchanges, prec))
	b.WriteString("\n")
	b.WriteString(stSection.Render("Cross-exchange spreads (buy at ask -> sell at bid)") + "\n")
	b.WriteString(m.spreadsTable(m.snap.spreads[m.selected], prec))
	b.WriteString("\n" + stDim.Render("  ←/→ or 1-9 switch symbol   q quit") + "\n")
	return b.String()
}

func (m model) header() string {
	status := stGreen.Render("● LIVE")
	if !m.snap.connected {
		msg := "connecting"
		if m.snap.lastErr != "" {
			msg = "reconnecting: " + m.snap.lastErr
		}
		status = stRed.Render("○ " + truncate(msg, 60))
	}
	stats := stDim.Render(fmt.Sprintf("%s   %.0f msg/s   exchange→client p50 %s p99 %s   server→client p50 %s",
		m.addr, m.rate, ms(m.snap.e2eP50), ms(m.snap.e2eP99), ms(m.snap.deliverP50)))
	return stTitle.Render("◆ Crypto Market Aggregator") + "  " + status + "  " + stats
}

func (m model) tabs() string {
	parts := make([]string, 0, len(m.snap.symbols))
	for i, s := range m.snap.symbols {
		label := fmt.Sprintf("%d %s", i+1, s)
		if s == m.selected {
			parts = append(parts, stTabSel.Render(label))
		} else {
			parts = append(parts, stTab.Render(label))
		}
	}
	return lipgloss.JoinHorizontal(lipgloss.Top, parts...)
}

func (m model) quotesTable(books map[string]*pb.OrderBook, exchanges []string, prec int) string {
	bestBid, bestAsk := math.Inf(-1), math.Inf(1)
	for _, b := range books {
		if len(b.Bids) > 0 {
			bestBid = max(bestBid, b.Bids[0].Price)
		}
		if len(b.Asks) > 0 {
			bestAsk = min(bestAsk, b.Asks[0].Price)
		}
	}

	var sb strings.Builder
	sb.WriteString(stDim.Render(fmt.Sprintf("  %-8s %14s %12s %14s %12s %10s %8s", "EXCHANGE", "BID", "BID SIZE", "ASK", "ASK SIZE", "SPREAD", "AGE")) + "\n")
	now := time.Now()
	for _, ex := range exchanges {
		b := books[ex]
		if len(b.Bids) == 0 || len(b.Asks) == 0 {
			continue
		}
		bid, ask := b.Bids[0], b.Asks[0]
		bidStr := fmt.Sprintf("%14s", fmtPrice(bid.Price, prec))
		askStr := fmt.Sprintf("%14s", fmtPrice(ask.Price, prec))
		if bid.Price == bestBid {
			bidStr = stBest.Render(bidStr)
		}
		if ask.Price == bestAsk {
			askStr = stBest.Render(askStr)
		}
		ownBps := (ask.Price - bid.Price) / ask.Price * 10_000
		fmt.Fprintf(&sb, "  %-8s %s %12s %s %12s %10s %s\n",
			stBold.Render(fmt.Sprintf("%-8s", ex)), bidStr, fmtSize(bid.Size), askStr, fmtSize(ask.Size),
			fmt.Sprintf("%.2f bps", ownBps), age(now, b.ExchangeTime.AsTime()))
	}
	return sb.String()
}

func (m model) orderBooks(books map[string]*pb.OrderBook, exchanges []string, prec int) string {
	cols := make([]string, 0, len(exchanges))
	for _, ex := range exchanges {
		b := books[ex]
		var lines []string
		lines = append(lines, stBold.Render(ex))
		asks := b.Asks[:min(m.depth, len(b.Asks))]
		for i := m.depth - 1; i >= 0; i-- { // best ask at the bottom, next to the bids
			if i >= len(asks) {
				lines = append(lines, "")
				continue
			}
			lines = append(lines, stRed.Render(fmt.Sprintf("%12s", fmtPrice(asks[i].Price, prec)))+" "+fmt.Sprintf("%11s", fmtSize(asks[i].Size)))
		}
		if len(b.Bids) > 0 && len(b.Asks) > 0 {
			gap := b.Asks[0].Price - b.Bids[0].Price
			lines = append(lines, stDim.Render(fmt.Sprintf("── %s ──", fmtPrice(gap, prec))))
		} else {
			lines = append(lines, stDim.Render("──"))
		}
		for i := 0; i < m.depth; i++ {
			if i >= len(b.Bids) {
				lines = append(lines, "")
				continue
			}
			lines = append(lines, stGreen.Render(fmt.Sprintf("%12s", fmtPrice(b.Bids[i].Price, prec)))+" "+fmt.Sprintf("%11s", fmtSize(b.Bids[i].Size)))
		}
		cols = append(cols, stBox.Render(strings.Join(lines, "\n")))
	}
	return lipgloss.JoinHorizontal(lipgloss.Top, cols...)
}

func (m model) spreadsTable(spreads map[string]*pb.Spread, prec int) string {
	list := make([]*pb.Spread, 0, len(spreads))
	for _, s := range spreads {
		list = append(list, s)
	}
	slices.SortFunc(list, func(a, b *pb.Spread) int { return cmp.Compare(b.SpreadBps, a.SpreadBps) })

	var sb strings.Builder
	sb.WriteString(stDim.Render(fmt.Sprintf("  %-8s %-8s %14s %14s %12s %10s %12s %8s", "BUY AT", "SELL AT", "BUY PRICE", "SELL PRICE", "SPREAD", "BPS", "VOLUME", "AGE")) + "\n")
	now := time.Now()
	for _, s := range list {
		bps := fmt.Sprintf("%+10.3f", s.SpreadBps)
		switch {
		case s.SpreadBps > 0:
			bps = stBest.Render(bps)
		default:
			bps = stRed.Render(bps)
		}
		fmt.Fprintf(&sb, "  %-8s %-8s %14s %14s %12s %s %12s %s\n",
			s.BuyExchange, s.SellExchange, fmtPrice(s.BuyPrice, prec), fmtPrice(s.SellPrice, prec),
			fmtPrice(s.Spread, prec), bps, fmtSize(s.Volume), age(now, s.Time.AsTime()))
	}
	if len(list) == 0 {
		sb.WriteString(stDim.Render("  need at least two exchanges with fresh data") + "\n")
	}
	return sb.String()
}

// pricePrecision finds how many decimals the prices of a symbol use, so
// that all columns are aligned (BTC has 1-2 decimals, DOGE has 5).
func pricePrecision(books map[string]*pb.OrderBook) int {
	prec := 0
	for _, b := range books {
		for _, l := range append(slices.Clone(b.Bids), b.Asks...) {
			s := strconv.FormatFloat(l.Price, 'f', -1, 64)
			if i := strings.IndexByte(s, '.'); i >= 0 {
				prec = max(prec, len(s)-i-1)
			}
		}
	}
	return min(prec, 8)
}

func fmtPrice(p float64, prec int) string { return strconv.FormatFloat(p, 'f', prec, 64) }

func fmtSize(s float64) string {
	switch {
	case s >= 100_000:
		return fmt.Sprintf("%.0f", s)
	case s >= 100:
		return fmt.Sprintf("%.2f", s)
	case s >= 0.01:
		return fmt.Sprintf("%.4f", s)
	default:
		return fmt.Sprintf("%.6f", s)
	}
}

func age(now, t time.Time) string {
	d := now.Sub(t)
	s := fmt.Sprintf("%8s", ms(d))
	switch {
	case d > staleAfter:
		return stRed.Render(s)
	case d > time.Second:
		return stYellow.Render(s)
	default:
		return stDim.Render(s)
	}
}

func ms(d time.Duration) string {
	if d >= 10*time.Second {
		return fmt.Sprintf("%.0fs", d.Seconds())
	}
	return fmt.Sprintf("%dms", d.Milliseconds())
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
