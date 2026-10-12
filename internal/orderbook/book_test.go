package orderbook

import (
	"slices"
	"testing"

	"github.com/w1xent/agregator/internal/model"
)

func TestBookSortsAndTrims(t *testing.T) {
	b := New()
	b.SetBid(100, 1)
	b.SetBid(102, 2)
	b.SetBid(101, 3)
	b.SetAsk(105, 1)
	b.SetAsk(103, 2)
	b.SetAsk(104, 3)

	wantBids := []model.Level{{Price: 102, Size: 2}, {Price: 101, Size: 3}}
	if got := b.Bids(2); !slices.Equal(got, wantBids) {
		t.Errorf("Bids = %v, want %v", got, wantBids)
	}
	wantAsks := []model.Level{{Price: 103, Size: 2}, {Price: 104, Size: 3}, {Price: 105, Size: 1}}
	if got := b.Asks(0); !slices.Equal(got, wantAsks) {
		t.Errorf("Asks = %v, want %v", got, wantAsks)
	}
}

func TestBookDeltaUpdatesAndDeletes(t *testing.T) {
	b := New()
	b.SetBid(100, 1)
	b.SetBid(99, 1)
	b.SetBid(100, 5) // update
	b.SetBid(99, 0)  // delete

	want := []model.Level{{Price: 100, Size: 5}}
	if got := b.Bids(0); !slices.Equal(got, want) {
		t.Errorf("Bids = %v, want %v", got, want)
	}

	b.Reset()
	if !b.Empty() {
		t.Error("book must be empty after Reset")
	}
}

func TestBookCrossed(t *testing.T) {
	b := New()
	b.SetBid(100, 1)
	b.SetAsk(101, 1)
	if b.Crossed() {
		t.Fatal("normal book reported as crossed")
	}
	b.SetAsk(99.5, 1)
	if !b.Crossed() {
		t.Fatal("crossed book not detected")
	}
}
