// Package retrybudget caps retries as a fraction of overall traffic, shared
// by every caller of a dependency, instead of a fixed "try N times" per call.
//
// # The problem
//
// A per-call retry limit ("try 3 times") is a reasonable rule for one call
// considered on its own, and a dangerous one considered across a fleet. When
// a dependency degrades, every caller multiplies its own load against the
// thing that is already failing: with attempts A and failure rate f, load
// becomes 1+f+f²+...+f^(A-1) times normal. The multiplier is smallest when
// the dependency is healthy and largest when it is dying, which is exactly
// backwards. That is how a partial outage, made of independently reasonable
// per-call retry policies, becomes a full one (a "metastable failure"; see
// Bronson et al., HotOS '21).
//
// A retry budget fixes this by capping retries as a ratio of the requests
// flowing through a shared window, not a count per call. If retries exceed
// the ratio, further retries are refused until the ratio recovers. It never
// blocks a first attempt — only the amplification a chain of retries would
// otherwise add.
//
// # Prior art, including inside the tools you already use
//
// This is not a novel idea. Google's SRE book specifies a client retry
// budget at 10%. Finagle ships a RetryBudget. Envoy has retry budgets in its
// circuit breaker settings. And grpc-go — the reference gRPC implementation
// for Go — contains exactly this mechanism internally: clientconn.go defines
// an unexported retryThrottler with max/threshold/ratio fields and
// throttle()/successfulRPC() methods, a token-bucket implementation of the
// same SRE-book model this package implements. It is driven only by gRPC
// service-config JSON (a "retryThrottling" policy) and is not exposed as a
// reusable API, importable only inside gRPC calls and only when a service
// config supplies the policy. That a major, independently-engineered RPC
// library re-implements this pattern privately is strong evidence the
// pattern is right; it is also the reason it is worth having as a
// general-purpose, dependency-free package rather than plumbing locked
// inside one RPC library.
//
// # What this package is not
//
//   - Not a circuit breaker. First attempts always go through; the budget
//     only removes the amplification retries add. Run a breaker alongside
//     it if you want load-shedding too.
//   - Not a rate limiter or concurrency limiter. It bounds the ratio of
//     retries to requests, nothing else.
//   - Not distributed. A [Budget] lives in one process's memory. This
//     composes better than it sounds — the ratio is scale-free, so N
//     processes each holding themselves to 10% put 10% onto the dependency
//     in aggregate — but it cannot pool a small allowance across processes,
//     or notice one shard failing while others are healthy. A distributed
//     budget needs a shared store (Redis, a sidecar) and the latency and
//     failure modes that come with one; that is deliberately out of scope
//     here.
package retrybudget
