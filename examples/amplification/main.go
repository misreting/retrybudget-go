// Command amplification simulates a dependency that briefly degrades, and
// compares the load it receives under three retry policies: none, a fixed
// per-call retry limit, and a shared retry budget. It uses the real
// [retrybudget.Budget] type for the budget policy — these are measured
// numbers from running the simulation, not a hand-computed table.
//
// Run it with:
//
//	go run ./examples/amplification
package main

import (
	"fmt"
	"sync"
	"time"

	retrybudget "github.com/__GH_OWNER__/retrybudget-go"
)

const (
	callers          = 190 // concurrent callers, each offering 1 req/s
	ticks            = 20  // simulated seconds
	normalCapacity   = 200 // requests/s the dependency can actually serve
	degradedCapacity = 60  // capacity during the bad window
	degradedStart    = 5   // first degraded tick (inclusive)
	degradedEnd      = 9   // last degraded tick (inclusive) — 5 seconds bad
	maxAttempts      = 3   // total attempts per logical call, incl. the first
)

func capacityAt(tick int) int {
	if tick >= degradedStart && tick <= degradedEnd {
		return degradedCapacity
	}
	return normalCapacity
}

func isDegraded(tick int) bool {
	return tick >= degradedStart && tick <= degradedEnd
}

// tickResult is what one simulated second produced under a given policy.
type tickResult struct {
	attempts int // total attempts the dependency received, incl. retries
	ok       int // attempts that succeeded (bounded by capacity)
}

// simulateNoRetries: every caller makes exactly one attempt per tick.
func simulateNoRetries() []tickResult {
	results := make([]tickResult, ticks)
	for t := 1; t <= ticks; t++ {
		capacity := capacityAt(t)
		ok := min(callers, capacity)
		results[t-1] = tickResult{attempts: callers, ok: ok}
	}
	return results
}

// simulatePerCallRetries: each caller retries up to maxAttempts times,
// immediately, against the same tick's capacity — the capacity is a shared
// pool for that second, consumed greedily by whichever round gets to it
// first, same as it would be by real concurrent callers.
func simulatePerCallRetries() []tickResult {
	results := make([]tickResult, ticks)
	for t := 1; t <= ticks; t++ {
		capacity := capacityAt(t)
		remaining := capacity
		alive := callers
		attempts, ok := 0, 0

		for round := 1; round <= maxAttempts && alive > 0; round++ {
			attempts += alive
			got := min(alive, remaining)
			ok += got
			remaining -= got
			alive -= got // survivors of this round retry next round
		}
		results[t-1] = tickResult{attempts: attempts, ok: ok}
	}
	return results
}

// simulateBudget: same shared per-tick capacity, but every attempt after
// the first must clear budget.TryRetry() against one real, shared Budget —
// exactly as a fleet of callers sharing one Budget would.
func simulateBudget(budget *retrybudget.Budget, clock *manualClock) []tickResult {
	results := make([]tickResult, ticks)
	for t := 1; t <= ticks; t++ {
		clock.set(time.Unix(int64(t), 0))
		capacity := capacityAt(t)
		remaining := capacity

		for i := 0; i < callers; i++ {
			budget.RecordRequest()
		}

		attempts := callers
		alive := callers
		ok := min(alive, remaining)
		remaining -= ok
		alive -= ok

		for round := 2; round <= maxAttempts && alive > 0; round++ {
			granted := 0
			for i := 0; i < alive; i++ {
				if budget.TryRetry() {
					granted++
				}
			}
			attempts += granted
			got := min(granted, remaining)
			ok += got
			remaining -= got
			alive = granted - got // granted but still failing: eligible for one more round
		}
		results[t-1] = tickResult{attempts: attempts, ok: ok}
	}
	return results
}

type manualClock struct {
	mu sync.Mutex
	t  time.Time
}

func newManualClock(start time.Time) *manualClock { return &manualClock{t: start} }
func (c *manualClock) now() time.Time             { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *manualClock) set(t time.Time)            { c.mu.Lock(); defer c.mu.Unlock(); c.t = t }

type summary struct {
	attempts       int
	ok             int
	peakPerSecond  int
	degradedAtmpts int
}

func summarize(results []tickResult) summary {
	var s summary
	for t, r := range results {
		s.attempts += r.attempts
		s.ok += r.ok
		if r.attempts > s.peakPerSecond {
			s.peakPerSecond = r.attempts
		}
		if isDegraded(t + 1) {
			s.degradedAtmpts += r.attempts
		}
	}
	return s
}

func main() {
	offered := callers * ticks

	none := summarize(simulateNoRetries())
	perCall := summarize(simulatePerCallRetries())

	clock := newManualClock(time.Unix(0, 0))
	budget, err := retrybudget.New(
		retrybudget.WithRatio(0.1),
		retrybudget.WithMinPerSecond(3),
		retrybudget.WithWindow(10*time.Second),
		retrybudget.WithClock(clock.now),
	)
	if err != nil {
		panic(err) // example code: a hardcoded, valid config never errors
	}
	budgetPolicy := summarize(simulateBudget(budget, clock))

	fmt.Printf("  dependency: %d req/s, dropping to %d req/s for seconds %d-%d\n",
		normalCapacity, degradedCapacity, degradedStart, degradedEnd)
	fmt.Printf("  offered:    %d callers x 1 req/s for %ds = %d calls\n\n", callers, ticks, offered)

	fmt.Printf("  %-22s %9s %9s %8s %16s %8s\n",
		"policy", "attempts", "overall", "peak/s", "while degraded", "ok")
	printRow("no retries", offered, none)
	printRow(fmt.Sprintf("per-call retries (%dx)", maxAttempts), offered, perCall)
	printRow("retry budget (10%)", offered, budgetPolicy)
}

func printRow(name string, offered int, s summary) {
	overall := float64(s.attempts) / float64(offered)
	fmt.Printf("  %-22s %9d %8.2fx %8d %16d %8d\n",
		name, s.attempts, overall, s.peakPerSecond, s.degradedAtmpts, s.ok)
}
