package retrybudget

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newFastTransport builds a Transport whose backoff is effectively instant,
// so retry tests run fast without needing to fake time.Sleep.
func newFastTransport(next http.RoundTripper, budget *Budget) *Transport {
	return &Transport{
		Next:   next,
		Budget: budget,
		Backoff: BackoffConfig{
			MinDelay: time.Microsecond,
			MaxDelay: time.Microsecond,
			Factor:   1,
		},
	}
}

func TestRoundTripperRetriesOn5xxThenSucceeds(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&hits, 1)
		if n < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	b, err := New()
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: newFastTransport(http.DefaultTransport, b)}

	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "ok" {
		t.Errorf("body = %q, want ok", body)
	}
	if hits != 3 {
		t.Errorf("hits = %d, want 3", hits)
	}
}

func TestRoundTripperGivesUpAfterMaxAttempts(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	b, err := New()
	if err != nil {
		t.Fatal(err)
	}
	transport := newFastTransport(http.DefaultTransport, b)
	transport.MaxAttempts = 3

	var reason GiveUpReason
	transport.OnGiveUp = func(attempt int, resp *http.Response, err error, r GiveUpReason) {
		reason = r
	}

	client := &http.Client{Transport: transport}
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
	if hits != 3 {
		t.Errorf("hits = %d, want 3", hits)
	}
	if reason != GiveUpReasonAttempts {
		t.Errorf("reason = %v, want attempts", reason)
	}
}

func TestRoundTripperDoesNotRetry4xx(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	b, err := New()
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: newFastTransport(http.DefaultTransport, b)}

	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if hits != 1 {
		t.Errorf("hits = %d, want 1 (a 400 must never be retried)", hits)
	}
}

func TestRoundTripperBudgetCapsRetries(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	// Ratio and floor both zero: no retry is ever permitted.
	b, err := New(WithRatio(0), WithMinPerSecond(0))
	if err != nil {
		t.Fatal(err)
	}
	transport := newFastTransport(http.DefaultTransport, b)
	transport.MaxAttempts = 5

	client := &http.Client{Transport: transport}
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	stats := b.Stats()
	if stats.Requests != 1 {
		t.Errorf("stats.Requests = %d, want 1", stats.Requests)
	}
	if stats.Retries != 0 {
		t.Errorf("stats.Retries = %d, want 0 (budget should have refused every retry)", stats.Retries)
	}
}

func TestRoundTripperRetriesPOSTBodyViaGetBody(t *testing.T) {
	var bodies []string
	var mu sync.Mutex
	var hits int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(body))
		mu.Unlock()

		if atomic.AddInt32(&hits, 1) < 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	b, err := New()
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: newFastTransport(http.DefaultTransport, b)}

	req, err := http.NewRequest(http.MethodPost, srv.URL, bytes.NewReader([]byte("payload")))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 2 {
		t.Fatalf("server saw %d requests, want 2", len(bodies))
	}
	for _, got := range bodies {
		if got != "payload" {
			t.Errorf("body = %q, want payload (GetBody should replay the same body each attempt)", got)
		}
	}
}

func TestRoundTripperUnreplayableBodyIsNotRetried(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	b, err := New()
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: newFastTransport(http.DefaultTransport, b)}

	pr, pw := io.Pipe()
	go func() {
		_, _ = pw.Write([]byte("payload"))
		pw.Close()
	}()

	req, err := http.NewRequest(http.MethodPost, srv.URL, pr)
	if err != nil {
		t.Fatal(err)
	}
	req.GetBody = nil // io.Pipe gives no GetBody; make it explicit for the test

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if hits != 1 {
		t.Errorf("hits = %d, want 1 (a body that can't be replayed must not be retried)", hits)
	}
}

func TestRoundTripperHonorsRetryAfterHeader(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&hits, 1) < 2 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	b, err := New()
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: newFastTransport(http.DefaultTransport, b)}

	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if hits != 2 {
		t.Errorf("hits = %d, want 2", hits)
	}
}

func TestRoundTripperContextCancellationStopsRetries(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	b, err := New()
	if err != nil {
		t.Fatal(err)
	}
	transport := newFastTransport(http.DefaultTransport, b)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already done before the first attempt returns

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := transport.RoundTrip(req)
	if err == nil {
		defer resp.Body.Close()
	}
	// Either the first attempt itself fails due to the cancelled context, or
	// it succeeds once and then the retry loop refuses to continue; both
	// are acceptable, but there must be no infinite retrying.
}

// TestRoundTripperConcurrent drives many concurrent requests through one
// Transport and shared Budget under -race.
func TestRoundTripperConcurrent(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&hits, 1)
		if n%3 == 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	b, err := New(WithRatio(0.2), WithWindow(time.Minute), WithBuckets(60))
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: newFastTransport(http.DefaultTransport, b)}

	const goroutines = 32
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				resp, err := client.Get(srv.URL)
				if err != nil {
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
		}()
	}
	wg.Wait()

	_ = b.Stats() // no assertion beyond "the race detector found nothing"
}
