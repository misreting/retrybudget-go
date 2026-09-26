package retrybudget

import (
	"math"
	"math/rand/v2"
	"net/http"
	"time"
)

// BackoffConfig controls the exponential-with-full-jitter backoff used
// between retry attempts.
type BackoffConfig struct {
	// MinDelay is the first backoff interval, before jitter. Default 100ms.
	MinDelay time.Duration
	// MaxDelay caps the interval, before jitter. Default 30s.
	MaxDelay time.Duration
	// Factor is the backoff multiplier applied per attempt. Default 2.
	Factor float64
}

func (c BackoffConfig) withDefaults() BackoffConfig {
	if c.MinDelay <= 0 {
		c.MinDelay = 100 * time.Millisecond
	}
	if c.MaxDelay <= 0 {
		c.MaxDelay = 30 * time.Second
	}
	if c.Factor <= 0 {
		c.Factor = 2
	}
	return c
}

// fullJitter computes the delay before the given retry attempt (1 being the
// first retry, i.e. the wait after the first failed attempt).
//
// It uses full jitter: the whole interval is random, not just part of it.
// The interval before jitter is min(MaxDelay, MinDelay*Factor^(attempt-1));
// the delay actually used is a uniform random draw from [0, interval].
//
// This is a deliberate choice over "equal jitter" (half fixed, half random).
// A fixed floor feels safer because it guarantees some spacing, but the
// fixed part is exactly what keeps a population of callers correlated: they
// failed together, so with equal jitter they'd wait out the same floor and
// come back together too, just with a softer edge on top of it. Full jitter
// spreads the same population across the entire interval instead, with
// nothing held in common. AWS's published measurements of this exact
// comparison put full jitter ahead on both total work done against the
// backing service and completion time, and it is the simpler rule of the
// two to reason about.
func fullJitter(attempt int, cfg BackoffConfig) time.Duration {
	cfg = cfg.withDefaults()
	if attempt < 1 {
		attempt = 1
	}

	interval := float64(cfg.MinDelay) * math.Pow(cfg.Factor, float64(attempt-1))
	if interval > float64(cfg.MaxDelay) {
		interval = float64(cfg.MaxDelay)
	}
	if interval <= 0 {
		return 0
	}

	return time.Duration(rand.Float64() * interval) //nolint:gosec // jitter, not a security use
}

// ParseRetryAfter parses a Retry-After header value per RFC 9110 §10.2.3: it
// is either a non-negative integer number of seconds, or an HTTP-date. It
// returns the duration to wait from now, and false if the value could not be
// parsed as either form.
func ParseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	if value == "" {
		return 0, false
	}

	if secs, ok := parseRetryAfterSeconds(value); ok {
		if secs < 0 {
			return 0, false
		}
		return time.Duration(secs) * time.Second, true
	}

	if t, err := http.ParseTime(value); err == nil {
		d := t.Sub(now)
		if d < 0 {
			d = 0
		}
		return d, true
	}

	return 0, false
}

func parseRetryAfterSeconds(value string) (int64, bool) {
	var n int64
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int64(c-'0')
	}
	if len(value) == 0 {
		return 0, false
	}
	return n, true
}

// spreadRetryAfter adds up to 10% jitter (capped at 1 second) on top of a
// Retry-After delay, so that many callers handed the same header value don't
// all come back in the same instant.
func spreadRetryAfter(d time.Duration) time.Duration {
	spread := d / 10
	if spread > time.Second {
		spread = time.Second
	}
	if spread <= 0 {
		return d
	}
	return d + time.Duration(rand.Float64()*float64(spread))
}
