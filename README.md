# retrybudget

Retries capped as a fraction of your traffic, shared by every caller of a dependency —
instead of "up to 3 times" per call.

```
  dependency: 200 req/s, dropping to 60 req/s for seconds 5-9
  offered:    190 callers x 1 req/s for 20s = 3800 calls

  policy                  attempts   overall   peak/s   while degraded       ok
  no retries                  3800     1.00x      190              950     3150
  per-call retries (3x)       5100     1.34x      450             2250     3150
  retry budget (10%)          3971     1.04x      285             1121     3150
```

That's `go run ./examples/amplification` — real output, pasted in below the fold with
the full command. All three policies deliver the same 3,150 successful calls: capacity
is capacity, and no retry policy can serve more than the dependency can actually handle.
What differs is the load thrown at it while it's struggling. Per-call retries pushed a
peak of 450 attempts/s at a dependency that was admitting 60 — more than seven times
over. The budget peaked at 285 and settled the whole run at 1.04x instead of 1.34x.

## The problem

Every general-purpose Go retry library caps retries **per call**. `sethvargo/go-retry`
takes a fixed retry count, `hashicorp/go-retryablehttp` takes `RetryMax`,
`avast/retry-go` takes `retry.Attempts(3)`. Each is a limit on one call's behavior, and
each is a reasonable number for one call considered on its own. `sony/gobreaker` is the
other thing people reach for here — it's a circuit breaker, a different mechanism (more
on that below).

None of them contains a number of callers.

Work it through. A service takes 1,000 req/s from 200 processes, every one of them
using a `maxAttempts: 3` retry policy. A bad deploy takes it to an 80% error rate. With
`A` attempts and a failure rate `f`, each caller multiplies its own load by
`1 + f + f² + … + f^(A-1)`:

| failure rate | load multiplier at `maxAttempts: 3` |
| --- | --- |
| 1% | 1.01x |
| 20% | 1.24x |
| 50% | 1.75x |
| 80% | 2.44x |
| 100% | 3.00x |

1,000 req/s becomes 2,440 req/s. The service is now serving a fifth of what it could and
receiving two and a half times as much as before. Nobody misconfigured anything. Every
caller respected its limit of three attempts. The limit is per call, and nothing limits
the number of calls.

Look at the shape of that column: **the multiplier is smallest when the dependency is
healthy and largest when it is dying.** A per-call retry limit adds load in exact
proportion to how badly the thing is already failing. And if the added load is by itself
enough to hold the failure rate up, the outage outlives its cause — the service stays
down after the bad deploy is rolled back, because the retries are the load now. (That's
a metastable failure; Bronson et al., HotOS '21.)

### Why the usual answers don't fix it

- **"Turn retries off."** Correct during the incident, wrong the other 99.9% of the
  time, when one retry papers over a connection reset. You can't be there to flip it,
  and the moment it needs flipping is the moment you have other things to do.
- **"Use 2 attempts instead of 3."** Moves the ceiling from 3x to 2x. The multiplier
  still rises as the dependency gets sicker, which is the actual defect.
- **"Exponential backoff with jitter."** Spreads retries out in time; does not reduce
  how many of them there are. Jitter addresses correlation, not volume. (This package
  does full jitter too — necessary, not sufficient. See below.)
- **"A circuit breaker."** The closest thing, worth having, and not the same thing. A
  breaker is a switch on an error-rate threshold: under it, all your traffic and all
  your amplification go through; over it, none of it does, including first attempts
  that would have succeeded. Its half-open probe is a small herd of its own. A budget
  never blocks a first attempt — it removes only the amplification, continuously, with
  no threshold to cross. Run both.
- **"A concurrency limit."** Bounds work in flight, not the ratio of wasted work to
  useful work; and a queue in front of the limit converts the amplification into latency
  rather than removing it.

## The fix already exists — just not as an importable API

The fix is old and well-tested: cap retries as a **fraction of requests** over a window,
shared by everything that talks to that dependency. Google's SRE book specifies a
client retry budget at 10%. Finagle ships a `RetryBudget`. Envoy has retry budgets in
its circuit breaker settings.

And **grpc-go — the reference gRPC implementation for Go — already has one, internally**.
`clientconn.go` defines:

```go
type retryThrottler struct {
	max    float64
	thresh float64
	ratio  float64
	mu     sync.Mutex
	tokens float64
}

func (rt *retryThrottler) throttle() bool
func (rt *retryThrottler) successfulRPC()
```

a token-bucket implementation of the same SRE-book model this package implements — a
`max` bucket size, a `thresh`-old below which throttling kicks in, and a `ratio` by
which the bucket refills on success and drains on a throttled retry. It is constructed
in `(*ClientConn).applyServiceConfigAndBalancer` only when a service config supplies a
`retryThrottling` policy:

```go
if cc.sc.retryThrottling != nil {
	newThrottler := &retryThrottler{
		tokens: cc.sc.retryThrottling.MaxTokens,
		max:    cc.sc.retryThrottling.MaxTokens,
		thresh: cc.sc.retryThrottling.MaxTokens / 2,
		ratio:  cc.sc.retryThrottling.TokenRatio,
	}
	cc.retryThrottler.Store(newThrottler)
}
```

`retryThrottler`, its fields, and both its methods are unexported. The only way to reach
it is through a gRPC channel and a `retryThrottling` block in service-config JSON — it
is not a type you can import, construct, or point at an `http.Client`, a database driver,
or any dependency that isn't itself a gRPC call.

Read that the way it deserves to be read: **the team that maintains gRPC for Go
independently arrived at exactly this mechanism and judged it necessary enough to ship**.
That is about as strong a piece of evidence as exists that the pattern is right. It is
also the argument for why it shouldn't stay locked inside one RPC library — every other
dependency a Go program calls (an HTTP API, Postgres, Redis, a gRPC service with no
`retryThrottling` configured) gets no equivalent, and has to reach for a per-call retry
library instead, with the amplification problem above intact.

This package is that mechanism, generalized: no service config, no gRPC requirement, and
a public API you can share across an `http.Client`, a database call, or anything else
that returns `(T, error)`.

## Install

```sh
go get github.com/agentscope-ai-java/retrybudget-go
```

Requires Go 1.27.1 (uses `math/rand/v2`; a lower Go 1.22+ toolchain would also work if
you lower the `go.mod` directive). No runtime dependencies — standard library only.

## Use

```go
import (
	"context"
	"net/http"
	"time"

	retrybudget "github.com/agentscope-ai-java/retrybudget-go"
)

// One budget per dependency, created once and shared by everything that calls it.
var payments, _ = retrybudget.New(
	retrybudget.WithRatio(0.1),        // retries may be at most 10% of requests
	retrybudget.WithMinPerSecond(3),   // ...but always allow a trickle, so quiet paths still retry
	retrybudget.WithWindow(10*time.Second),
)

val, err := retrybudget.Do(ctx, payments, func(ctx context.Context) (*Invoice, error) {
	return client.FetchInvoice(ctx, id)
}, retrybudget.CallOptions{
	MaxAttempts: 3,
	ShouldRetry: func(err error) bool { return isTransient(err) },
})
```

A budget constructed per call shares nothing and caps nothing — it has to be a
package-level singleton, one per dependency. Whether that is per host, per service, or
per endpoint is a judgement call: the budget is only meaningful over calls that fail
*together*, so one per service is usually right.

`Do` returns whatever the last attempt returned — `err == nil` on success, otherwise
the last error, unwrapped, so `errors.Is`/`errors.As` keep working.

```go
retrybudget.CallOptions{
	OnGiveUp: func(attempt int, err error, reason retrybudget.GiveUpReason) {
		metrics.Incr("retry.give_up." + string(reason))
	},
}
```

`reason` is one of `"attempts"`, `"predicate"`, `"budget"`, `"retry-after"`, or
`"context"`. `GiveUpReasonBudget` is the one to alert on: it means the dependency is
failing widely enough, across every caller sharing the budget, that retrying further
would be the bigger problem.

### Options

On `New`, and shared by everything using that `*Budget`:

| Option | Default | |
| --- | --- | --- |
| `WithRatio` | `0.1` | Retries permitted as a fraction of requests in the window. |
| `WithMinPerSecond` | `1` | Retries per second permitted whatever the ratio says. |
| `WithWindow` | `10s` | Span of the sliding window. |
| `WithBuckets` | `20` | Ring size. Expiry granularity is `window / buckets`. |
| `WithClock` | `time.Now` | Clock, replaceable so simulations and tests run deterministically. |

Per call, via `CallOptions`:

| Field | Default | |
| --- | --- | --- |
| `MaxAttempts` | `3` | Total attempts including the first. `1` disables retrying. |
| `ShouldRetry` | everything but `context.Canceled`/`DeadlineExceeded` | Is this error worth retrying? |
| `RetryAfter` | — | Read a raw `Retry-After` value off the error. |
| `MaxRetryAfter` | `60s` | A longer wait than this is treated as a refusal to retry. |
| `Backoff` | see below | `MinDelay` 100ms, `MaxDelay` 30s, `Factor` 2. |
| `OnRetry` / `OnGiveUp` | — | Observability hooks. |
| `Sleep` | respects `ctx` | Replaceable so tests don't sleep for real. |

Narrow `ShouldRetry`. Retrying a 400 is always wrong: it will never succeed, and it
spends budget that a retryable failure somewhere else needed.

### Watching it

```go
stats := payments.Stats()
// retrybudget.Stats{Requests:812, Retries:74, Ratio:0.091, Allowance:81.2,
//                    Remaining:7.2, Permitted:true, Window:10s}
```

`Ratio` and `Permitted` are the two to graph. A `Ratio` that rides at the configured cap
means either the dependency is unhealthy or `ShouldRetry` is too broad; `Permitted`
going `false` is the budget doing its job, and is worth an alert.

`CanRetry()` answers the same question without consuming anything; `TryRetry()` is what
to call before actually retrying, since a `CanRetry` check followed by a separate retry
is racy — two goroutines can both see "yes" for the same unit of allowance. `TryRetry`
checks and consumes atomically under one lock.

### The floor

A pure ratio starves exactly the services that can afford retries. At 4 req/s, 10% is
0.4 retries per second — so a path that sees one connection reset an hour never gets to
retry it, though a single retry there is both harmless and the entire reason you wanted
retries. `WithMinPerSecond` is a second, independent allowance:

```
allowance = max(minPerSecond * windowSeconds, requests * ratio)
```

Whichever is more generous wins, so the floor governs when traffic is thin and the ratio
takes over as soon as there is enough traffic for it to mean anything. With the defaults
(`0.1`, `1`/s, `10s` window) the ratio only starts to bind above 100 req/s.

### Full jitter, not equal jitter

Backoff is `random() × min(maxDelay, minDelay×factor^(n-1))` — the whole interval is
random, with no fixed floor.

The variant people reach for first is "equal jitter", half fixed and half random,
because a delay that can come back at almost 0ms feels wrong. But the fixed half is
precisely the part that keeps callers correlated: they failed together, so with equal
jitter they wait out the same floor and return together too, just with a softer edge.
Full jitter spreads the same population across the entire interval instead. AWS's
published measurements of exactly this comparison put full jitter ahead on both total
work done against the backing service and completion time, and it is the simpler rule
of the two to reason about.

### Retry-After

`RetryAfter` is a function so this package never has to know what your transport looks
like:

```go
retrybudget.CallOptions{
	RetryAfter: func(err error) (string, bool) {
		var e *myHTTPError
		if errors.As(err, &e) {
			return e.Header.Get("Retry-After"), true
		}
		return "", false
	},
}
```

Per RFC 9110 §10.2.3, a plain integer is seconds and a string may also be an HTTP-date;
`ParseRetryAfter` handles both. A value that parses replaces the backoff entirely, plus
up to 10% of spread (capped at 1s), so a thousand clients handed the same
`Retry-After: 2` don't all come back in the same millisecond. A value over
`MaxRetryAfter` is treated as a refusal: the call gives up now rather than holding a
retry slot open for an hour.

### `http.RoundTripper`

```go
client := &http.Client{
	Transport: &retrybudget.Transport{
		Next:   http.DefaultTransport, // or your own instrumented transport
		Budget: payments,
		ShouldRetry: func(resp *http.Response, err error) bool {
			return err != nil || resp.StatusCode >= 500 || resp.StatusCode == 429
		},
	},
}
```

Drop-in: it retries through the shared budget, replays a request body via `GetBody`
when one is available, refuses to retry a body it can't replay (`GetBody == nil`, e.g. a
raw `io.Reader` with no way to rewind — retrying that would risk sending an unintended
duplicate), and reads a `Retry-After` response header the same way `RetryAfter` does
above.

### Driving your own loop

If the retrying is already written, the budget works standalone:

```go
budget.RecordRequest()
for attempt := 1; attempt <= 3; attempt++ {
	resp, err := call()
	if err == nil {
		return resp, nil
	}
	if attempt == 3 || !budget.TryRetry() {
		return resp, err
	}
	time.Sleep(backoff(attempt))
}
```

## Concurrency

A `*Budget` is safe for concurrent use. Internally it holds one `sync.Mutex`, taken only
for the duration of a counter update or a totals sum — never across a network call, a
sleep, or any caller-supplied function. `go test -race ./...` passes, including tests
that hammer one shared `Budget` from dozens of goroutines and assert the ratio invariant
holds exactly (not approximately) under real concurrent load — see
`TestBudgetConcurrent`, `TestDoConcurrent`, and `TestRoundTripperConcurrent`.

## Memory

The window is a ring of bucket counters, not a list of timestamps, so a budget costs the
same regardless of throughput. `slidingWindow.recordRequest`/`recordRetry` never
allocate (`advance` only ever writes into the existing `[]windowBucket` backing array);
reading a total is `O(buckets)`. The cost of that bound is granularity: counts leave the
window a bucket at a time, so the span actually covered at any instant is between
`window - window/buckets` and `window` — with the defaults (10s window, 20 buckets)
that's 9.5s to 10s, far finer than any retry policy needs.

## What it does not do

- **It is per-process.** A `*Budget` lives in one process's memory. This composes
  better than it sounds — the ratio is scale-free, so N processes each holding
  themselves to 10% put 10% onto the dependency in aggregate, which is not true of a
  per-process *absolute* limit. What it cannot do is pool a small allowance usefully
  across many processes, or notice that one shard is in trouble while the others
  aren't. A distributed budget needs a shared store (Redis, a sidecar) and the latency
  and failure modes that come with one — deliberately out of scope here.
- **It is not a circuit breaker.** First attempts always go through, however bad things
  get. The budget removes amplification; it does not shed load. Run both.
- **It is not a rate limiter or a concurrency limiter.** It bounds the ratio of retries
  to requests, nothing else.
- **It does not know what is safe to retry.** Idempotency is your call, via
  `ShouldRetry`/`Transport.ShouldRetry` — this package will happily replay a `POST` if
  you tell it to.
- **No hedging.** Retries here are sequential; there is no speculative second request
  against a slow one. That is a different policy with a different budget.
- **Nothing is HTTP-specific in the core.** `Budget` and `Do` know nothing about status
  codes or headers; `Transport` and `ParseRetryAfter` are the HTTP-flavored pieces built
  on top, and are the only places that know what a `Retry-After` header looks like.
- **The window is a window.** A burst can spend the whole allowance in a fraction of it
  — in the simulation above, a 10% budget still peaked at 285 attempts/s against an
  offered 190. Shorten `WithWindow` if you want the cap to bind more tightly in time.

## Develop

```sh
go build ./...
go vet ./...
go test -race ./...
gofmt -l .                    # must print nothing
go run ./examples/amplification
```

## License

MIT
