package retrybudget

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// manualClock is a controllable, monotonic-enough clock for deterministic
// budget tests.
type manualClock struct {
	mu sync.Mutex
	t  time.Time
}

func newManualClock(start time.Time) *manualClock {
	return &manualClock{t: start}
}

func (c *manualClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *manualClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func TestNewValidation(t *testing.T) {
	tests := []struct {
		name    string
		opts    []Option
		wantErr bool
	}{
		{"defaults", nil, false},
		{"zero window", []Option{WithWindow(0)}, true},
		{"negative window", []Option{WithWindow(-time.Second)}, true},
		{"zero buckets", []Option{WithBuckets(0)}, true},
		{"negative ratio", []Option{WithRatio(-0.1)}, true},
		{"negative floor", []Option{WithMinPerSecond(-1)}, true},
		{"nil clock", []Option{WithClock(nil)}, true},
		{"valid custom", []Option{WithRatio(0.2), WithMinPerSecond(5), WithWindow(5 * time.Second), WithBuckets(5)}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := New(tt.opts...)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if b != nil {
					t.Error("expected nil Budget on error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if b == nil {
				t.Fatal("expected non-nil Budget")
			}
		})
	}
}

func TestBudgetRatio(t *testing.T) {
	clock := newManualClock(time.Unix(0, 0))
	b, err := New(
		WithRatio(0.1),
		WithMinPerSecond(0),
		WithWindow(10*time.Second),
		WithBuckets(10),
		WithClock(clock.now),
	)
	if err != nil {
		t.Fatal(err)
	}

	// 100 requests => allowance = 100 * 0.1 = 10 retries.
	for i := 0; i < 100; i++ {
		b.RecordRequest()
	}

	allowed := 0
	for i := 0; i < 20; i++ {
		if b.TryRetry() {
			allowed++
		}
	}
	if allowed != 10 {
		t.Errorf("allowed retries = %d, want 10", allowed)
	}

	stats := b.Stats()
	if stats.Requests != 100 {
		t.Errorf("stats.Requests = %d, want 100", stats.Requests)
	}
	if stats.Retries != 10 {
		t.Errorf("stats.Retries = %d, want 10", stats.Retries)
	}
	if stats.Permitted {
		t.Error("stats.Permitted = true, want false once allowance is exhausted")
	}
	if got, want := stats.Ratio, 0.1; got != want {
		t.Errorf("stats.Ratio = %v, want %v", got, want)
	}
}

func TestBudgetFloor(t *testing.T) {
	clock := newManualClock(time.Unix(0, 0))
	// At low traffic, the ratio would allow ~0 retries; the floor should
	// still grant a trickle.
	b, err := New(
		WithRatio(0.1),
		WithMinPerSecond(1), // 1/s * 10s window = 10 retries floor
		WithWindow(10*time.Second),
		WithBuckets(10),
		WithClock(clock.now),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Only 2 requests: ratio alone would give 0.2 retries.
	b.RecordRequest()
	b.RecordRequest()

	allowed := 0
	for i := 0; i < 20; i++ {
		if b.TryRetry() {
			allowed++
		}
	}
	if allowed != 10 {
		t.Errorf("allowed retries = %d, want 10 (floor should govern)", allowed)
	}
}

func TestBudgetExpiry(t *testing.T) {
	clock := newManualClock(time.Unix(0, 0))
	b, err := New(
		WithRatio(0.5),
		WithMinPerSecond(0),
		WithWindow(10*time.Second),
		WithBuckets(10),
		WithClock(clock.now),
	)
	if err != nil {
		t.Fatal(err)
	}

	b.RecordRequest()
	if !b.TryRetry() {
		t.Fatal("expected first retry to be permitted")
	}

	clock.advance(11 * time.Second)
	stats := b.Stats()
	if stats.Requests != 0 || stats.Retries != 0 {
		t.Errorf("after full window expiry: requests=%d retries=%d, want 0,0", stats.Requests, stats.Retries)
	}
}

func TestBudgetCanRetryDoesNotConsume(t *testing.T) {
	clock := newManualClock(time.Unix(0, 0))
	b, err := New(WithRatio(1), WithMinPerSecond(0), WithClock(clock.now))
	if err != nil {
		t.Fatal(err)
	}
	b.RecordRequest()

	for i := 0; i < 5; i++ {
		if !b.CanRetry() {
			t.Fatalf("CanRetry() = false on iteration %d, want true", i)
		}
	}
	if got := b.Stats().Retries; got != 0 {
		t.Errorf("CanRetry consumed budget: retries = %d, want 0", got)
	}
}

// TestBudgetConcurrent exercises RecordRequest/TryRetry/Stats from many
// goroutines at once under -race, and checks the invariant a budget exists
// to enforce: retries granted never exceed the allowance the recorded
// requests justify, however many goroutines raced to ask.
func TestBudgetConcurrent(t *testing.T) {
	b, err := New(WithRatio(0.1), WithMinPerSecond(0), WithWindow(time.Minute), WithBuckets(60))
	if err != nil {
		t.Fatal(err)
	}

	const goroutines = 64
	const requestsPerG = 200

	var wg sync.WaitGroup
	var grantedRetries int64

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < requestsPerG; i++ {
				b.RecordRequest()
				if b.TryRetry() {
					atomic.AddInt64(&grantedRetries, 1)
				}
				_ = b.CanRetry()
				_ = b.Stats()
			}
		}()
	}
	wg.Wait()

	totalRequests := int64(goroutines * requestsPerG)
	stats := b.Stats()

	if stats.Requests != totalRequests {
		t.Errorf("stats.Requests = %d, want %d", stats.Requests, totalRequests)
	}
	if int64(grantedRetries) != stats.Retries {
		t.Errorf("granted retries observed = %d, stats.Retries = %d, want equal", grantedRetries, stats.Retries)
	}

	// allowance(requests) is monotonically non-decreasing as requests only
	// grows over the run (well within the window), and TryRetry only ever
	// grants a retry when retries < allowance(requests-so-far). So the
	// total granted can never exceed allowance computed at the final
	// request count — this is a hard cap, not a statistical one.
	maxAllowed := float64(totalRequests) * 0.1
	if float64(grantedRetries) > maxAllowed {
		t.Errorf("granted %d retries, want at most %.0f (10%% of %d requests)", grantedRetries, maxAllowed, totalRequests)
	}
}
