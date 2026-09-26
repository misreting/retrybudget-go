package retrybudget

import (
	"net/http"
	"testing"
	"time"
)

func TestFullJitterBounds(t *testing.T) {
	cfg := BackoffConfig{MinDelay: 100 * time.Millisecond, MaxDelay: time.Second, Factor: 2}

	for attempt := 1; attempt <= 10; attempt++ {
		interval := float64(cfg.MinDelay) * pow(cfg.Factor, float64(attempt-1))
		if interval > float64(cfg.MaxDelay) {
			interval = float64(cfg.MaxDelay)
		}
		for i := 0; i < 200; i++ {
			d := fullJitter(attempt, cfg)
			if d < 0 {
				t.Fatalf("attempt %d: delay %v < 0", attempt, d)
			}
			if float64(d) > interval {
				t.Fatalf("attempt %d: delay %v exceeds interval %v", attempt, d, time.Duration(interval))
			}
		}
	}
}

func pow(base, exp float64) float64 {
	result := 1.0
	for i := 0; i < int(exp); i++ {
		result *= base
	}
	return result
}

func TestFullJitterDefaults(t *testing.T) {
	d := fullJitter(1, BackoffConfig{})
	if d < 0 || d > 100*time.Millisecond {
		t.Errorf("default first delay = %v, want in [0, 100ms]", d)
	}
}

func TestFullJitterVaries(t *testing.T) {
	// Full jitter should not degenerate to a constant value across calls.
	seen := map[time.Duration]bool{}
	for i := 0; i < 50; i++ {
		seen[fullJitter(5, BackoffConfig{MinDelay: 100 * time.Millisecond, MaxDelay: 30 * time.Second, Factor: 2})] = true
	}
	if len(seen) < 10 {
		t.Errorf("fullJitter produced only %d distinct values across 50 calls, expected more spread", len(seen))
	}
}

func TestParseRetryAfterSeconds(t *testing.T) {
	now := time.Unix(1000, 0)
	d, ok := ParseRetryAfter("5", now)
	if !ok || d != 5*time.Second {
		t.Errorf("ParseRetryAfter(5) = %v, %v; want 5s, true", d, ok)
	}
}

func TestParseRetryAfterHTTPDate(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	future := now.Add(30 * time.Second)
	header := future.Format(http.TimeFormat)

	d, ok := ParseRetryAfter(header, now)
	if !ok {
		t.Fatal("expected HTTP-date to parse")
	}
	if d < 29*time.Second || d > 31*time.Second {
		t.Errorf("ParseRetryAfter(date) = %v, want ~30s", d)
	}
}

func TestParseRetryAfterInvalid(t *testing.T) {
	tests := []string{"", "not-a-number-or-date", "-5"}
	for _, v := range tests {
		if _, ok := ParseRetryAfter(v, time.Now()); ok {
			t.Errorf("ParseRetryAfter(%q) = ok, want not ok", v)
		}
	}
}

func TestSpreadRetryAfter(t *testing.T) {
	base := 10 * time.Second
	for i := 0; i < 100; i++ {
		d := spreadRetryAfter(base)
		if d < base {
			t.Fatalf("spreadRetryAfter(%v) = %v, want >= base", base, d)
		}
		if d > base+time.Second {
			t.Fatalf("spreadRetryAfter(%v) = %v, want <= base+1s cap", base, d)
		}
	}
}

func TestSpreadRetryAfterSmallValue(t *testing.T) {
	base := 2 * time.Second // 10% = 200ms, under the 1s cap
	for i := 0; i < 100; i++ {
		d := spreadRetryAfter(base)
		if d < base || d > base+200*time.Millisecond {
			t.Fatalf("spreadRetryAfter(%v) = %v, out of expected range", base, d)
		}
	}
}
