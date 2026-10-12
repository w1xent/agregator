package grpcapi

import (
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/w1xent/agregator/gen/marketdata/v1"
	"github.com/w1xent/agregator/internal/model"
)

// ToProtoBook converts a model book to protobuf. published may be shared by
// many messages because messages are never changed after creation.
func ToProtoBook(b *model.OrderBook, published *timestamppb.Timestamp) *pb.OrderBook {
	return &pb.OrderBook{
		Exchange:      b.Exchange,
		Symbol:        b.Symbol,
		Bids:          toProtoLevels(b.Bids),
		Asks:          toProtoLevels(b.Asks),
		ExchangeTime:  timestamppb.New(b.ExchangeTS),
		ReceivedTime:  timestamppb.New(b.ReceivedTS),
		PublishedTime: published,
		Sequence:      b.Seq,
	}
}

func toProtoLevels(levels []model.Level) []*pb.PriceLevel {
	out := make([]*pb.PriceLevel, len(levels))
	for i, l := range levels {
		out[i] = &pb.PriceLevel{Price: l.Price, Size: l.Size}
	}
	return out
}

// ToProtoSpread converts a model spread to protobuf.
func ToProtoSpread(s *model.Spread, published *timestamppb.Timestamp) *pb.Spread {
	return &pb.Spread{
		Symbol:        s.Symbol,
		BuyExchange:   s.BuyExchange,
		SellExchange:  s.SellExchange,
		BuyPrice:      s.BuyPrice,
		SellPrice:     s.SellPrice,
		Spread:        s.Spread,
		SpreadBps:     s.SpreadBps,
		Volume:        s.Volume,
		Time:          timestamppb.New(s.Time),
		PublishedTime: published,
	}
}

// trimBook returns a book with at most depth levels per side. The original
// message is shared between streams, so a new message is created instead
// of changing it.
func trimBook(b *pb.OrderBook, depth int) *pb.OrderBook {
	if depth <= 0 || (len(b.Bids) <= depth && len(b.Asks) <= depth) {
		return b
	}
	return &pb.OrderBook{
		Exchange:      b.Exchange,
		Symbol:        b.Symbol,
		Bids:          b.Bids[:min(depth, len(b.Bids))],
		Asks:          b.Asks[:min(depth, len(b.Asks))],
		ExchangeTime:  b.ExchangeTime,
		ReceivedTime:  b.ReceivedTime,
		PublishedTime: b.PublishedTime,
		Sequence:      b.Sequence,
	}
}
