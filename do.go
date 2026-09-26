package retrybudget

import (
	"context"
	"errors"
	"time"
)

// GiveUpReason identifies why Do stopped retrying and returned its last
// result. GiveUpReasonBudget is the one worth alerting on: it means the
// dependency is failing widely enough, across every caller sharing the
// budget, that retrying further would itself be the bigger problem.
type GiveUpReason string

const (
	// GiveUpReasonNone is the zero value; it is never passed to OnGiveUp.
	GiveUpReasonNone GiveUpReason = ""
	// GiveUpReasonAttempts means MaxAttempts was reached.
	GiveUpReasonAttempts GiveUpReason = "attempts"
	// GiveUpReasonPredicate means ShouldRetry returned false for the error.
	GiveUpReasonPredicate GiveUpReason = "predicate"
	// GiveUpReasonBudget means the shared Budget refused the retry.
	GiveUpReasonBudget GiveUpReason = "budget"
	// GiveUpReasonRetryAfter means the server asked for a wait longer than
	// MaxRetryAfter, treated as a refusal to retry rather than a long sleep.
	GiveUpReasonRetryAfter GiveUpReason = "retry-after"
	// GiveUpReasonContext means ctx was done before another attempt started.
	GiveUpReasonContext GiveUpReason = "context"
)

// CallOptions configures a single call made through Do. The zero value is
// usable: DefaultMaxAttempts, a ShouldRetry that retries anything except
// context cancellation, and default backoff apply.
type CallOptions struct {
	// MaxAttempts is the total number of attempts, including the first.
	// 1 disables retrying entirely. Default 3.
	MaxAttempts int

	// ShouldRetry reports whether the given error is worth retrying. The
	// default retries everything except context.Canceled and
	// context.DeadlineExceeded. Narrow this: retrying a non-retryable error
	// (a 400, a validation failure) never succeeds and spends budget a
	// retryable failure elsewhere needed.
	ShouldRetry func(err error) bool

	// RetryAfter, if set, extracts a raw Retry-After header value (or
	// equivalent) from an error, per RFC 9110 §10.2.3: a plain integer is
	// seconds, a string may also be an HTTP-date. When it returns ok, the
	// parsed duration replaces the computed backoff for that attempt (plus
	// a little jitter of its own), and a value over MaxRetryAfter is
	// treated as a refusal to retry.
	RetryAfter func(err error) (value string, ok bool)

	// MaxRetryAfter caps how long a Retry-After value will be honored.
	// Default 60s.
	MaxRetryAfter time.Duration

	// Backoff configures the exponential-with-full-jitter delay used when
	// RetryAfter does not apply.
	Backoff BackoffConfig

	// OnRetry, if set, is called just before waiting to retry.
	OnRetry func(attempt int, err error, delay time.Duration)

	// OnGiveUp, if set, is called when Do is about to return its last
	// result without retrying further.
	OnGiveUp func(attempt int, err error, reason GiveUpReason)

	// Sleep waits for d, or returns ctx.Err() if ctx is done first. The
	// default respects ctx cancellation via a time.Timer. Replaceable so
	// tests and simulations can run against a virtual clock.
	Sleep func(ctx context.Context, d time.Duration) error
}

func (o CallOptions) withDefaults() CallOptions {
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 3
	}
	if o.ShouldRetry == nil {
		o.ShouldRetry = defaultShouldRetry
	}
	if o.MaxRetryAfter <= 0 {
		o.MaxRetryAfter = 60 * time.Second
	}
	if o.Sleep == nil {
		o.Sleep = defaultSleep
	}
	return o
}

func defaultShouldRetry(err error) bool {
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

func defaultSleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Do runs fn, retrying on error through the given Budget until fn succeeds,
// attempts run out, opts.ShouldRetry declines the error, or the Budget
// refuses the retry. It records one request against b per call to Do
// (regardless of how many attempts that call makes), and consumes one unit
// of the budget's allowance per retry actually taken.
//
// Do returns whatever the last attempt returned: fn's result and error
// unmodified. Callers can distinguish "gave up" from "succeeded" with the
// usual err == nil check; opts.OnGiveUp additionally reports why retrying
// stopped, which is the signal worth alerting on when the reason is
// GiveUpReasonBudget.
func Do[T any](ctx context.Context, b *Budget, fn func(ctx context.Context) (T, error), opts CallOptions) (T, error) {
	opts = opts.withDefaults()

	if b != nil {
		b.RecordRequest()
	}

	var (
		val T
		err error
	)
	for attempt := 1; ; attempt++ {
		val, err = fn(ctx)
		if err == nil {
			return val, nil
		}

		if ctxErr := ctx.Err(); ctxErr != nil {
			giveUp(opts, attempt, err, GiveUpReasonContext)
			return val, err
		}

		if attempt >= opts.MaxAttempts {
			giveUp(opts, attempt, err, GiveUpReasonAttempts)
			return val, err
		}

		if !opts.ShouldRetry(err) {
			giveUp(opts, attempt, err, GiveUpReasonPredicate)
			return val, err
		}

		delay := fullJitter(attempt, opts.Backoff)
		if opts.RetryAfter != nil {
			if raw, ok := opts.RetryAfter(err); ok {
				d, parsed := ParseRetryAfter(raw, time.Now())
				if parsed {
					if d > opts.MaxRetryAfter {
						giveUp(opts, attempt, err, GiveUpReasonRetryAfter)
						return val, err
					}
					delay = spreadRetryAfter(d)
				}
			}
		}

		if b != nil && !b.TryRetry() {
			giveUp(opts, attempt, err, GiveUpReasonBudget)
			return val, err
		}

		if opts.OnRetry != nil {
			opts.OnRetry(attempt, err, delay)
		}

		if sleepErr := opts.Sleep(ctx, delay); sleepErr != nil {
			return val, sleepErr
		}
	}
}

func giveUp(opts CallOptions, attempt int, err error, reason GiveUpReason) {
	if opts.OnGiveUp != nil {
		opts.OnGiveUp(attempt, err, reason)
	}
}
