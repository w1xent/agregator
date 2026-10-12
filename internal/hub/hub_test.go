package hub

import (
	"sync/atomic"
	"testing"
)

func TestPublishWithFilter(t *testing.T) {
	h := New[int](10, nil)
	all := h.Subscribe(nil)
	even := h.Subscribe(func(v int) bool { return v%2 == 0 })
	defer h.Unsubscribe(all)
	defer h.Unsubscribe(even)

	for i := range 4 {
		h.Publish(i)
	}
	if len(all.C()) != 4 {
		t.Errorf("all got %d values, want 4", len(all.C()))
	}
	if len(even.C()) != 2 {
		t.Errorf("even got %d values, want 2", len(even.C()))
	}
	if v := <-even.C(); v != 0 {
		t.Errorf("first even value = %d", v)
	}
}

func TestSlowSubscriberDropsInsteadOfBlocking(t *testing.T) {
	var drops atomic.Int64
	h := New[int](2, func() { drops.Add(1) })
	slow := h.Subscribe(nil) // never reads
	fast := h.Subscribe(nil)

	for i := range 5 {
		h.Publish(i) // must not block although "slow" is full
		if v := <-fast.C(); v != i {
			t.Fatalf("fast got %d, want %d", v, i)
		}
	}

	if slow.Dropped() != 3 || drops.Load() != 3 {
		t.Errorf("slow dropped %d (hook %d), want 3", slow.Dropped(), drops.Load())
	}
	if fast.Dropped() != 0 {
		t.Errorf("fast dropped %d, want 0", fast.Dropped())
	}
}

func TestUnsubscribe(t *testing.T) {
	h := New[string](1, nil)
	s := h.Subscribe(nil)
	if h.Len() != 1 {
		t.Fatalf("Len = %d", h.Len())
	}
	h.Unsubscribe(s)
	h.Publish("x")
	if h.Len() != 0 || len(s.C()) != 0 {
		t.Fatal("unsubscribed subscriber still receives values")
	}
}
