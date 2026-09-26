package retrybudget

import (
	"fmt"
	"io"
	"net/http"
	"time"
)

// Transport is an http.RoundTripper that retries failed requests through a
// shared Budget, so a struggling backend receives a bounded fraction of
// extra load from retries no matter how many callers share this Transport
// or how many other Transports share the same Budget.
//
// The zero value is usable except for Budget, which should normally be set
// so retries are actually capped; a nil Budget makes Transport retry with
// backoff alone, with no shared cap.
type Transport struct {
	// Next is the underlying RoundTripper. Defaults to http.DefaultTransport.
	Next http.RoundTripper

	// Budget caps retries as a fraction of requests made through this
	// Transport. Share one Budget across every Transport (and every other
	// caller) that talks to the same dependency.
	Budget *Budget

	// MaxAttempts is the total number of attempts, including the first.
	// Default 3. Forced to 1 for any request whose body cannot be rewound
	// (Body set with GetBody nil), since replaying it would send a request
	// the caller never asked to send twice.
	MaxAttempts int

	// ShouldRetry reports whether a response/error pair is worth retrying.
	// The default retries transport errors and 5xx or 429 responses.
	// Retrying a 4xx other than 429 is almost always wrong: it will not
	// succeed, and it spends budget a retryable failure elsewhere needed.
	ShouldRetry func(resp *http.Response, err error) bool

	// MaxRetryAfter caps how long a Retry-After response header will be
	// honored. Default 60s. A larger value is treated as a refusal to
	// retry rather than a long wait.
	MaxRetryAfter time.Duration

	// Backoff configures the exponential-with-full-jitter delay used when
	// the response carries no usable Retry-After header.
	Backoff BackoffConfig

	// OnRetry, if set, is called just before waiting to retry.
	OnRetry func(attempt int, resp *http.Response, err error, delay time.Duration)

	// OnGiveUp, if set, is called when RoundTrip is about to return its
	// last response or error without retrying further.
	OnGiveUp func(attempt int, resp *http.Response, err error, reason GiveUpReason)
}

func defaultShouldRetryResponse(resp *http.Response, err error) bool {
	if err != nil {
		return true
	}
	if resp == nil {
		return false
	}
	return resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests
}

func (t *Transport) next() http.RoundTripper {
	if t.Next != nil {
		return t.Next
	}
	return http.DefaultTransport
}

// RoundTrip implements http.RoundTripper.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	maxAttempts := t.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	shouldRetry := t.ShouldRetry
	if shouldRetry == nil {
		shouldRetry = defaultShouldRetryResponse
	}
	maxRetryAfter := t.MaxRetryAfter
	if maxRetryAfter <= 0 {
		maxRetryAfter = 60 * time.Second
	}

	// A request body that can't be replayed can't safely be retried: doing
	// so would send the caller's request body twice to a server that may
	// not be idempotent about it.
	if req.Body != nil && req.GetBody == nil {
		maxAttempts = 1
	}

	if t.Budget != nil {
		t.Budget.RecordRequest()
	}

	ctx := req.Context()

	for attempt := 1; ; attempt++ {
		attemptReq := req
		if attempt > 1 && req.Body != nil {
			body, err := req.GetBody()
			if err != nil {
				return nil, fmt.Errorf("retrybudget: rewinding request body for retry: %w", err)
			}
			clone := req.Clone(ctx)
			clone.Body = body
			attemptReq = clone
		}

		resp, err := t.next().RoundTrip(attemptReq)
		if !shouldRetry(resp, err) {
			return resp, err
		}

		if ctxErr := ctx.Err(); ctxErr != nil {
			t.giveUp(attempt, resp, err, GiveUpReasonContext)
			return resp, err
		}
		if attempt >= maxAttempts {
			t.giveUp(attempt, resp, err, GiveUpReasonAttempts)
			return resp, err
		}

		delay := fullJitter(attempt, t.Backoff)
		if resp != nil {
			if raw := resp.Header.Get("Retry-After"); raw != "" {
				if d, ok := ParseRetryAfter(raw, time.Now()); ok {
					if d > maxRetryAfter {
						t.giveUp(attempt, resp, err, GiveUpReasonRetryAfter)
						return resp, err
					}
					delay = spreadRetryAfter(d)
				}
			}
		}

		if t.Budget != nil && !t.Budget.TryRetry() {
			t.giveUp(attempt, resp, err, GiveUpReasonBudget)
			return resp, err
		}

		if t.OnRetry != nil {
			t.OnRetry(attempt, resp, err, delay)
		}

		// We're committed to retrying: this response (if any) is being
		// discarded, so drain and close it to allow connection reuse
		// before making the next attempt.
		drainAndClose(resp)

		if sleepErr := defaultSleep(ctx, delay); sleepErr != nil {
			return nil, sleepErr
		}
	}
}

func (t *Transport) giveUp(attempt int, resp *http.Response, err error, reason GiveUpReason) {
	if t.OnGiveUp != nil {
		t.OnGiveUp(attempt, resp, err, reason)
	}
}

func drainAndClose(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}
