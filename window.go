package retrybudget

import "time"

// slidingWindow counts requests and retries over a trailing span of time
// using a fixed ring of bucket counters, rather than a list of timestamps.
// Its memory footprint is O(buckets) regardless of throughput: recording an
// event never allocates, and reading a total is O(buckets).
//
// The tradeoff for that bound is granularity: an event leaves the window a
// whole bucket at a time, so the span actually represented at any instant is
// between window-window/buckets and window, not exactly window. That is far
// finer than any retry policy needs.
//
// Not safe for concurrent use on its own; callers (Budget) must hold a lock
// around advance-then-read/write sequences.
type slidingWindow struct {
	bucketDuration time.Duration
	buckets        []windowBucket

	epoch     time.Time // time.Time of bucket index 0
	curIdx    int64     // current absolute bucket number since epoch
	haveEpoch bool
}

type windowBucket struct {
	requests int64
	retries  int64
}

func newSlidingWindow(window time.Duration, buckets int) *slidingWindow {
	return &slidingWindow{
		bucketDuration: window / time.Duration(buckets),
		buckets:        make([]windowBucket, buckets),
	}
}

// advance rotates the ring forward to now, zeroing any buckets that have
// fallen out of the window since the last call. It must be called before
// every read or write so stale counts expire lazily.
func (w *slidingWindow) advance(now time.Time) {
	if !w.haveEpoch {
		w.epoch = now
		w.haveEpoch = true
		return
	}

	elapsed := now.Sub(w.epoch)
	if elapsed < 0 {
		// Clock went backwards (e.g. a non-monotonic now func in tests).
		// Treat as no time having passed rather than corrupting the ring.
		return
	}

	newIdx := int64(elapsed / w.bucketDuration)
	diff := newIdx - w.curIdx
	if diff <= 0 {
		return
	}

	n := int64(len(w.buckets))
	if diff >= n {
		// Every bucket is stale (or the window was idle for a very long
		// time): clear the whole ring in O(buckets) rather than looping
		// once per elapsed bucket, which would be O(diff).
		for i := range w.buckets {
			w.buckets[i] = windowBucket{}
		}
	} else {
		for i := w.curIdx + 1; i <= newIdx; i++ {
			w.buckets[i%n] = windowBucket{}
		}
	}
	// Jump curIdx directly to newIdx: it must always equal the true bucket
	// number for "now", not merely have been incremented up to n times,
	// otherwise the next advance() would see a stale, still-huge diff
	// against a curIdx that never caught up and would keep re-clearing the
	// ring on every call even once real time stopped moving.
	w.curIdx = newIdx
}

func (w *slidingWindow) recordRequest(now time.Time) {
	w.advance(now)
	w.buckets[w.curIdx%int64(len(w.buckets))].requests++
}

func (w *slidingWindow) recordRetry(now time.Time) {
	w.advance(now)
	w.buckets[w.curIdx%int64(len(w.buckets))].retries++
}

// totals returns the sum of requests and retries currently in the window.
func (w *slidingWindow) totals(now time.Time) (requests, retries int64) {
	w.advance(now)
	for _, b := range w.buckets {
		requests += b.requests
		retries += b.retries
	}
	return requests, retries
}
