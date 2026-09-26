package retrybudget

import (
	"testing"
	"time"
)

func TestSlidingWindowBasic(t *testing.T) {
	w := newSlidingWindow(10*time.Second, 10)
	base := time.Unix(0, 0)

	w.recordRequest(base)
	w.recordRequest(base)
	w.recordRetry(base)

	requests, retries := w.totals(base)
	if requests != 2 {
		t.Errorf("requests = %d, want 2", requests)
	}
	if retries != 1 {
		t.Errorf("retries = %d, want 1", retries)
	}
}

func TestSlidingWindowExpiry(t *testing.T) {
	w := newSlidingWindow(10*time.Second, 10) // 1s per bucket
	base := time.Unix(0, 0)

	w.recordRequest(base)
	w.recordRequest(base.Add(500 * time.Millisecond))

	requests, _ := w.totals(base.Add(500 * time.Millisecond))
	if requests != 2 {
		t.Fatalf("requests before expiry = %d, want 2", requests)
	}

	// Move fully past the window: everything should have expired.
	requests, retries := w.totals(base.Add(11 * time.Second))
	if requests != 0 || retries != 0 {
		t.Errorf("after full expiry: requests=%d retries=%d, want 0,0", requests, retries)
	}
}

func TestSlidingWindowPartialExpiry(t *testing.T) {
	w := newSlidingWindow(10*time.Second, 10) // 1s buckets
	base := time.Unix(0, 0)

	// One request per second for 10 seconds (10 requests across 10 buckets).
	for i := 0; i < 10; i++ {
		w.recordRequest(base.Add(time.Duration(i) * time.Second))
	}
	requests, _ := w.totals(base.Add(9 * time.Second))
	if requests != 10 {
		t.Fatalf("requests = %d, want 10", requests)
	}

	// Advance by 3 more seconds: the first 3 buckets (t=0,1,2) should expire.
	requests, _ = w.totals(base.Add(12 * time.Second))
	if requests != 7 {
		t.Errorf("requests after partial expiry = %d, want 7", requests)
	}
}

func TestSlidingWindowClockGoingBackwards(t *testing.T) {
	w := newSlidingWindow(10*time.Second, 10)
	base := time.Unix(1000, 0)

	w.recordRequest(base)
	// A now() earlier than the epoch (e.g. a bad clock) must not panic or
	// corrupt state; it should be a no-op advance.
	w.recordRequest(base.Add(-5 * time.Second))

	requests, _ := w.totals(base)
	if requests != 2 {
		t.Errorf("requests = %d, want 2", requests)
	}
}

func TestSlidingWindowLongIdleThenResume(t *testing.T) {
	w := newSlidingWindow(10*time.Second, 10)
	base := time.Unix(0, 0)

	w.recordRequest(base)
	// Idle for far longer than the window; the ring must fully reset
	// without needing to loop once per elapsed bucket indefinitely.
	far := base.Add(1000 * time.Hour)
	w.recordRequest(far)

	requests, _ := w.totals(far)
	if requests != 1 {
		t.Errorf("requests = %d, want 1 (only the post-idle request)", requests)
	}
}
