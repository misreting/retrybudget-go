package retrybudget

import (
	"fmt"
	"sync"
	"time"
)

// Default configuration, matched to the values recommended by the SRE book
// and used by the Node reference implementation this package mirrors.
const (
	DefaultRatio        = 0.1
	DefaultMinPerSecond = 1.0
	DefaultWindow       = 10 * time.Second
	DefaultBuckets      = 20
)

// Budget caps retries as a fraction of the requests recorded through it over
// a trailing window. Construct one per dependency and share it across every
// caller of that dependency — a budget built per call shares nothing and
// caps nothing.
//
// A Budget is safe for concurrent use by multiple goroutines. The lock it
// holds internally is only ever held for the duration of a counter update or
// a totals sum; it is never held across a network call, a sleep, or any
// caller-supplied function.
type Budget struct {
	ratio        float64
	minPerSecond float64
	window       time.Duration
	now          func() time.Time

	mu  sync.Mutex
	win *slidingWindow
}

// Option configures a Budget constructed with New.
type Option func(*config)

type config struct {
	ratio        float64
	minPerSecond float64
	window       time.Duration
	buckets      int
	now          func() time.Time
}

// WithRatio sets the fraction of requests in the window that may be retries.
// The default is 0.1 (10%), the figure the SRE book recommends.
func WithRatio(ratio float64) Option {
	return func(c *config) { c.ratio = ratio }
}

// WithMinPerSecond sets a floor on retries per second that applies whatever
// the ratio says, so low-traffic paths still get a trickle of retries. See
// [Budget.Stats] for how the floor and the ratio combine. The default is 1.
func WithMinPerSecond(perSecond float64) Option {
	return func(c *config) { c.minPerSecond = perSecond }
}

// WithWindow sets the span of the trailing window over which requests and
// retries are counted. The default is 10 seconds.
func WithWindow(window time.Duration) Option {
	return func(c *config) { c.window = window }
}

// WithBuckets sets the number of ring-buffer buckets the window is divided
// into. More buckets give finer expiry granularity (window/buckets) at the
// cost of a little more memory and a little more work per read; the memory
// cost is bounded regardless of traffic. The default is 20.
func WithBuckets(buckets int) Option {
	return func(c *config) { c.buckets = buckets }
}

// WithClock overrides the clock a Budget uses, for deterministic tests and
// simulations. The default is time.Now.
func WithClock(now func() time.Time) Option {
	return func(c *config) { c.now = now }
}

// New creates a Budget from the given options, applying defaults for any
// left unset. It returns an error rather than panicking if the resulting
// configuration is invalid (a non-positive window, fewer than one bucket, or
// a negative ratio or floor).
func New(opts ...Option) (*Budget, error) {
	c := config{
		ratio:        DefaultRatio,
		minPerSecond: DefaultMinPerSecond,
		window:       DefaultWindow,
		buckets:      DefaultBuckets,
		now:          time.Now,
	}
	for _, opt := range opts {
		opt(&c)
	}

	if c.window <= 0 {
		return nil, fmt.Errorf("retrybudget: window must be positive, got %s", c.window)
	}
	if c.buckets < 1 {
		return nil, fmt.Errorf("retrybudget: buckets must be at least 1, got %d", c.buckets)
	}
	if c.ratio < 0 {
		return nil, fmt.Errorf("retrybudget: ratio must not be negative, got %v", c.ratio)
	}
	if c.minPerSecond < 0 {
		return nil, fmt.Errorf("retrybudget: minPerSecond must not be negative, got %v", c.minPerSecond)
	}
	if c.now == nil {
		return nil, fmt.Errorf("retrybudget: clock must not be nil")
	}

	return &Budget{
		ratio:        c.ratio,
		minPerSecond: c.minPerSecond,
		window:       c.window,
		now:          c.now,
		win:          newSlidingWindow(c.window, c.buckets),
	}, nil
}

// allowance returns the number of retries currently permitted, computed as
// max(minPerSecond * windowSeconds, requests * ratio): the floor and the
// ratio are independently satisfiable, and whichever is more generous wins.
// With the defaults, the ratio only starts to bind above 100 req/s; below
// that, the floor governs so a quiet path still gets to retry occasionally.
func (b *Budget) allowance(requests int64) float64 {
	floor := b.minPerSecond * b.window.Seconds()
	fromRatio := float64(requests) * b.ratio
	if fromRatio > floor {
		return fromRatio
	}
	return floor
}

// RecordRequest records one request against the budget. Call it once per
// call attempt through the dependency the budget guards — typically once per
// logical operation, not once per retry attempt of that operation.
func (b *Budget) RecordRequest() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.win.recordRequest(b.now())
}

// TryRetry reports whether a retry is currently permitted and, if so,
// consumes one unit of the budget's allowance atomically with that check —
// two goroutines racing TryRetry cannot both be told yes for the same unit
// of allowance. Callers that decide to retry should call TryRetry rather
// than CanRetry followed by a separate record, precisely to avoid that race.
func (b *Budget) TryRetry() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.now()
	requests, retries := b.win.totals(now)
	if float64(retries) >= b.allowance(requests) {
		return false
	}
	b.win.recordRetry(now)
	return true
}

// CanRetry reports whether a retry is currently permitted, without
// consuming any of the allowance. It answers "is the budget open" for
// display or logging; TryRetry is the method to call before actually
// retrying, since a CanRetry check followed by a separate retry is racy.
func (b *Budget) CanRetry() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	requests, retries := b.win.totals(b.now())
	return float64(retries) < b.allowance(requests)
}

// Stats is a point-in-time snapshot of a Budget's state, suitable for
// exposing on a metrics endpoint. Ratio and Permitted are the two worth
// graphing: a Ratio riding at the configured cap means the dependency is
// unhealthy (or the retry predicate is too broad), and Permitted going false
// is the budget doing its job — and worth an alert, since it means retries
// are being refused.
type Stats struct {
	// Requests is the number of requests recorded in the current window.
	Requests int64
	// Retries is the number of retries recorded in the current window.
	Retries int64
	// Ratio is Retries/Requests for the current window, or 0 if Requests is 0.
	Ratio float64
	// Allowance is the number of retries currently permitted in the window.
	Allowance float64
	// Remaining is Allowance-Retries; it can be negative transiently at the
	// instant a burst consumes the whole allowance.
	Remaining float64
	// Permitted reports whether TryRetry would currently succeed.
	Permitted bool
	// Window is the configured span of the trailing window.
	Window time.Duration
}

// Stats returns a snapshot of the budget's current state.
func (b *Budget) Stats() Stats {
	b.mu.Lock()
	defer b.mu.Unlock()

	requests, retries := b.win.totals(b.now())
	allowance := b.allowance(requests)

	var ratio float64
	if requests > 0 {
		ratio = float64(retries) / float64(requests)
	}

	return Stats{
		Requests:  requests,
		Retries:   retries,
		Ratio:     ratio,
		Allowance: allowance,
		Remaining: allowance - float64(retries),
		Permitted: float64(retries) < allowance,
		Window:    b.window,
	}
}
