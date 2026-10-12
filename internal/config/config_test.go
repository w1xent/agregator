package config

import (
	"slices"
	"testing"
	"time"
)

func TestEnvReadsValuesAndDefaults(t *testing.T) {
	env := NewFromMap(map[string]string{
		"NAME":    "collector",
		"DEPTH":   "15",
		"TIMEOUT": "250ms",
		"DEBUG":   "true",
		"LIST":    " a, b ,,c ",
		"EMPTY":   "  ",
	})

	if got := env.String("NAME", "x"); got != "collector" {
		t.Errorf("String = %q", got)
	}
	if got := env.String("EMPTY", "def"); got != "def" {
		t.Errorf("empty value must fall back to default, got %q", got)
	}
	if got := env.Int("DEPTH", 1); got != 15 {
		t.Errorf("Int = %d", got)
	}
	if got := env.Int("MISSING", 7); got != 7 {
		t.Errorf("Int default = %d", got)
	}
	if got := env.Duration("TIMEOUT", time.Second); got != 250*time.Millisecond {
		t.Errorf("Duration = %v", got)
	}
	if got := env.Bool("DEBUG", false); !got {
		t.Errorf("Bool = %v", got)
	}
	if got := env.List("LIST", nil); !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Errorf("List = %v", got)
	}
	if err := env.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEnvCollectsErrors(t *testing.T) {
	env := NewFromMap(map[string]string{"A": "abc", "B": "10 parsecs", "C": "maybe"})
	env.Int("A", 0)
	env.Duration("B", 0)
	env.Bool("C", false)
	if err := env.Err(); err == nil {
		t.Fatal("expected an error")
	}
}
