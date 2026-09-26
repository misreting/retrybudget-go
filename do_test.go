package retrybudget

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var errTransient = errors.New("transient failure")
var errPermanent = errors.New("permanent failure")

func instantSleep(_ context.Context, _ time.Duration) error { return nil }

func TestDoSucceedsFirstTry(t *testing.T) {
	b, err := New()
	if err != nil {
		t.Fatal(err)
	}

	var calls int32
	val, err := Do(context.Background(), b, func(ctx context.Context) (string, error) {
		atomic.AddInt32(&calls, 1)
		return "ok", nil
	}, CallOptions{Sleep: instantSleep})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "ok" {
		t.Errorf("val = %q, want ok", val)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1", calls)
	}
}

func TestDoRetriesThenSucceeds(t *testing.T) {
	b, err := New()
	if err != nil {
		t.Fatal(err)
	}

	var calls int32
	val, err := Do(context.Background(), b, func(ctx context.Context) (int, error) {
		n := atomic.AddInt32(&calls, 1)
		if n < 3 {
			return 0, errTransient
		}
		return 42, nil
	}, CallOptions{MaxAttempts: 5, Sleep: instantSleep})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 42 {
		t.Errorf("val = %d, want 42", val)
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3", calls)
	}
}

func TestDoGivesUpAfterMaxAttempts(t *testing.T) {
	b, err := New()
	if err != nil {
		t.Fatal(err)
	}

	var reasons []GiveUpReason
	_, err = Do(context.Background(), b, func(ctx context.Context) (int, error) {
		return 0, errTransient
	}, CallOptions{
		MaxAttempts: 3,
		Sleep:       instantSleep,
		OnGiveUp: func(attempt int, err error, reason GiveUpReason) {
			reasons = append(reasons, reason)
		},
	})

	if !errors.Is(err, errTransient) {
		t.Fatalf("err = %v, want errTransient", err)
	}
	if len(reasons) != 1 || reasons[0] != GiveUpReasonAttempts {
		t.Errorf("reasons = %v, want [attempts]", reasons)
	}
}

func TestDoPredicateStopsRetry(t *testing.T) {
	b, err := New()
	if err != nil {
		t.Fatal(err)
	}

	var calls int32
	var reason GiveUpReason
	_, err = Do(context.Background(), b, func(ctx context.Context) (int, error) {
		atomic.AddInt32(&calls, 1)
		return 0, errPermanent
	}, CallOptions{
		MaxAttempts: 5,
		Sleep:       instantSleep,
		ShouldRetry: func(err error) bool { return !errors.Is(err, errPermanent) },
		OnGiveUp:    func(attempt int, err error, r GiveUpReason) { reason = r },
	})

	if !errors.Is(err, errPermanent) {
		t.Fatalf("err = %v, want errPermanent", err)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1 (predicate should stop before any retry)", calls)
	}
	if reason != GiveUpReasonPredicate {
		t.Errorf("reason = %v, want predicate", reason)
	}
}

func TestDoBudgetRefusesRetry(t *testing.T) {
	// Ratio and floor both zero: the very first retry attempt is refused.
	b, err := New(WithRatio(0), WithMinPerSecond(0))
	if err != nil {
		t.Fatal(err)
	}

	var calls int32
	var reason GiveUpReason
	_, err = Do(context.Background(), b, func(ctx context.Context) (int, error) {
		atomic.AddInt32(&calls, 1)
		return 0, errTransient
	}, CallOptions{
		MaxAttempts: 5,
		Sleep:       instantSleep,
		OnGiveUp:    func(attempt int, err error, r GiveUpReason) { reason = r },
	})

	if !errors.Is(err, errTransient) {
		t.Fatalf("err = %v, want errTransient", err)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1 (budget should refuse the first retry)", calls)
	}
	if reason != GiveUpReasonBudget {
		t.Errorf("reason = %v, want budget", reason)
	}
}

type retryAfterError struct {
	value string
}

func (e *retryAfterError) Error() string { return "retry after: " + e.value }

func TestDoRetryAfterOverridesBackoff(t *testing.T) {
	b, err := New()
	if err != nil {
		t.Fatal(err)
	}

	var delays []time.Duration
	var calls int32
	_, _ = Do(context.Background(), b, func(ctx context.Context) (int, error) {
		n := atomic.AddInt32(&calls, 1)
		if n < 3 {
			return 0, &retryAfterError{value: "1"}
		}
		return 0, nil
	}, CallOptions{
		MaxAttempts: 5,
		Sleep: func(ctx context.Context, d time.Duration) error {
			delays = append(delays, d)
			return nil
		},
		RetryAfter: func(err error) (string, bool) {
			var rae *retryAfterError
			if errors.As(err, &rae) {
				return rae.value, true
			}
			return "", false
		},
	})

	if len(delays) != 2 {
		t.Fatalf("delays = %v, want 2 entries", delays)
	}
	for _, d := range delays {
		// 1s +/- up to 10% spread, per spreadRetryAfter.
		if d < time.Second || d > 1100*time.Millisecond {
			t.Errorf("delay = %v, want within [1s, 1.1s]", d)
		}
	}
}

func TestDoRetryAfterTooLongGivesUp(t *testing.T) {
	b, err := New()
	if err != nil {
		t.Fatal(err)
	}

	var reason GiveUpReason
	_, err = Do(context.Background(), b, func(ctx context.Context) (int, error) {
		return 0, &retryAfterError{value: "3600"} // 1 hour
	}, CallOptions{
		MaxAttempts:   5,
		Sleep:         instantSleep,
		MaxRetryAfter: 60 * time.Second,
		RetryAfter: func(err error) (string, bool) {
			var rae *retryAfterError
			if errors.As(err, &rae) {
				return rae.value, true
			}
			return "", false
		},
		OnGiveUp: func(attempt int, err error, r GiveUpReason) { reason = r },
	})

	if err == nil {
		t.Fatal("expected error")
	}
	if reason != GiveUpReasonRetryAfter {
		t.Errorf("reason = %v, want retry-after", reason)
	}
}

func TestDoRespectsContextCancellation(t *testing.T) {
	b, err := New()
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var calls int32
	_, err = Do(ctx, b, func(ctx context.Context) (int, error) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			cancel()
		}
		return 0, errTransient
	}, CallOptions{MaxAttempts: 5, Sleep: instantSleep})

	if err == nil {
		t.Fatal("expected error after cancellation")
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1 (should stop once ctx is done)", calls)
	}
}

func TestDoDefaultShouldRetrySkipsContextErrors(t *testing.T) {
	if defaultShouldRetry(context.Canceled) {
		t.Error("defaultShouldRetry(context.Canceled) = true, want false")
	}
	if defaultShouldRetry(context.DeadlineExceeded) {
		t.Error("defaultShouldRetry(context.DeadlineExceeded) = true, want false")
	}
	if !defaultShouldRetry(errTransient) {
		t.Error("defaultShouldRetry(errTransient) = false, want true")
	}
}

// TestDoConcurrent runs many concurrent Do calls against one shared Budget
// under -race, verifying no data race and that RecordRequest/TryRetry
// accounting stays consistent under real concurrent load.
func TestDoConcurrent(t *testing.T) {
	b, err := New(WithRatio(0.2), WithWindow(time.Minute), WithBuckets(60))
	if err != nil {
		t.Fatal(err)
	}

	const goroutines = 32
	const callsPerG = 50

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for i := 0; i < callsPerG; i++ {
				n := 0
				_, _ = Do(context.Background(), b, func(ctx context.Context) (int, error) {
					n++
					if n < 2 {
						return 0, errTransient
					}
					return 1, nil
				}, CallOptions{MaxAttempts: 3, Sleep: instantSleep})
			}
		}(g)
	}
	wg.Wait()

	stats := b.Stats()
	if stats.Requests != int64(goroutines*callsPerG) {
		t.Errorf("stats.Requests = %d, want %d", stats.Requests, goroutines*callsPerG)
	}
}
