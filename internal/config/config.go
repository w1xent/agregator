// Package config reads settings from environment variables.
//
// Every getter takes a default value. Parse errors are collected and
// returned by Err, so a service can report all bad variables at once.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Env reads typed values from environment variables.
type Env struct {
	lookup func(string) (string, bool)
	errs   []error
}

// New returns an Env that reads the process environment.
func New() *Env { return &Env{lookup: os.LookupEnv} }

// NewFromMap returns an Env that reads values from a map (useful in tests).
func NewFromMap(m map[string]string) *Env {
	return &Env{lookup: func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}}
}

func (e *Env) get(key string) (string, bool) {
	v, ok := e.lookup(key)
	if !ok {
		return "", false
	}
	v = strings.TrimSpace(v)
	return v, v != ""
}

// String returns the value of key or def.
func (e *Env) String(key, def string) string {
	if v, ok := e.get(key); ok {
		return v
	}
	return def
}

// Int returns the value of key as int or def.
func (e *Env) Int(key string, def int) int {
	v, ok := e.get(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %q is not an integer", key, v))
		return def
	}
	return n
}

// Duration returns the value of key as time.Duration (for example "5s") or def.
func (e *Env) Duration(key string, def time.Duration) time.Duration {
	v, ok := e.get(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %q is not a duration", key, v))
		return def
	}
	return d
}

// Bool returns the value of key as bool or def.
func (e *Env) Bool(key string, def bool) bool {
	v, ok := e.get(key)
	if !ok {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %q is not a boolean", key, v))
		return def
	}
	return b
}

// List returns a comma separated value of key as a slice or def.
// Empty items are skipped.
func (e *Env) List(key string, def []string) []string {
	v, ok := e.get(key)
	if !ok {
		return def
	}
	var out []string
	for item := range strings.SplitSeq(v, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	if len(out) == 0 {
		return def
	}
	return out
}

// Err returns all parse errors joined together, or nil.
func (e *Env) Err() error { return errors.Join(e.errs...) }
