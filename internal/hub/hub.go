// Package hub is a small in-memory publish/subscribe broker that sends
// updates from the aggregator to many gRPC streams.
//
// Publish never blocks: every subscriber has its own buffer, and if a slow
// client does not read fast enough, new messages for this client are
// dropped. A slow client must not slow down the whole service.
package hub

import (
	"sync"
	"sync/atomic"
)

// Hub fans out values of type T to subscribers.
type Hub[T any] struct {
	bufSize int
	onDrop  func()

	mu   sync.RWMutex
	subs map[*Subscription[T]]struct{}
}

// Subscription receives values from a Hub.
type Subscription[T any] struct {
	ch      chan T
	filter  func(T) bool
	dropped atomic.Uint64
}

// C returns the channel with published values.
func (s *Subscription[T]) C() <-chan T { return s.ch }

// Dropped returns how many values were dropped because the buffer was full.
func (s *Subscription[T]) Dropped() uint64 { return s.dropped.Load() }

// New creates a hub. bufSize is the buffer of every subscriber. onDrop is
// called for every dropped value and may be nil.
func New[T any](bufSize int, onDrop func()) *Hub[T] {
	if onDrop == nil {
		onDrop = func() {}
	}
	return &Hub[T]{bufSize: bufSize, onDrop: onDrop, subs: make(map[*Subscription[T]]struct{})}
}

// Subscribe adds a subscriber. filter may be nil to receive everything.
// Always call Unsubscribe when done.
func (h *Hub[T]) Subscribe(filter func(T) bool) *Subscription[T] {
	s := &Subscription[T]{ch: make(chan T, h.bufSize), filter: filter}
	h.mu.Lock()
	h.subs[s] = struct{}{}
	h.mu.Unlock()
	return s
}

// Unsubscribe removes the subscriber. The channel is not closed, because a
// concurrent Publish could still try to write to it.
func (h *Hub[T]) Unsubscribe(s *Subscription[T]) {
	h.mu.Lock()
	delete(h.subs, s)
	h.mu.Unlock()
}

// Publish sends v to every matching subscriber without blocking.
func (h *Hub[T]) Publish(v T) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for s := range h.subs {
		if s.filter != nil && !s.filter(v) {
			continue
		}
		select {
		case s.ch <- v:
		default:
			s.dropped.Add(1)
			h.onDrop()
		}
	}
}

// Len returns the number of subscribers.
func (h *Hub[T]) Len() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subs)
}
