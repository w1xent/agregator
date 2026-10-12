package exchange

import (
	"testing"
)

func TestChunk(t *testing.T) {
	got := Chunk([]int{1, 2, 3, 4, 5}, 2)
	if len(got) != 3 || len(got[0]) != 2 || len(got[2]) != 1 || got[2][0] != 5 {
		t.Fatalf("Chunk = %v", got)
	}
	if got := Chunk([]int{}, 3); len(got) != 0 {
		t.Fatalf("Chunk(empty) = %v", got)
	}
}

func TestParseLevels(t *testing.T) {
	lv, err := ParseLevels([][]string{{"1.5", "2"}, {"1.4", "3", "0", "7"}})
	if err != nil || len(lv) != 2 || lv[1].Price != 1.4 || lv[1].Size != 3 {
		t.Fatalf("levels=%v err=%v", lv, err)
	}
	if _, err := ParseLevels([][]string{{"abc", "1"}}); err == nil {
		t.Fatal("expected error for bad price")
	}
	if _, err := ParseLevels([][]string{{"1"}}); err == nil {
		t.Fatal("expected error for short level")
	}
}
