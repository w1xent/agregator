// Package exchange contains the common WebSocket logic for all exchange
// connectors: connect, subscribe, keep the connection alive, parse messages
// and reconnect after any failure.
//
// Each exchange only has to implement the small Adapter interface with its
// own URL, message formats and parsing rules.
package exchange

import (
	"errors"
	"fmt"
	"time"

	"github.com/w1xent/agregator/internal/model"
)

// Adapter knows the WebSocket protocol of one exchange.
type Adapter interface {
	// Name is a short lower case exchange name, for example "bybit".
	Name() string
	// URL is the public WebSocket endpoint.
	URL() string
	// SubscribeMessages returns the messages that must be sent right after
	// the connection is open. Symbols are canonical, like "BTC-USDT".
	SubscribeMessages(symbols []string) ([][]byte, error)
	// PingMessage is an application level ping that keeps the connection alive.
	PingMessage() []byte
	// Reset is called before every new connection. Adapters that keep local
	// state (for example books built from deltas) must clear it here.
	Reset()
	// Parse decodes one raw message. Service messages (pong, subscribe
	// confirmations) return no books and no error.
	Parse(raw []byte, received time.Time) ([]model.OrderBook, error)
}

// ErrResync means the local state of the adapter is broken (for example a
// crossed book). The stream reconnects to get a fresh snapshot.
var ErrResync = errors.New("order book is out of sync")

// Error is an error message that was sent by the exchange itself,
// for example "invalid symbol".
type Error struct {
	Exchange string
	Code     string
	Message  string
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: exchange error %s: %s", e.Exchange, e.Code, e.Message)
}
