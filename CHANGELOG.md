# Changelog

## [Unreleased]

### Fixed

- After a heartbeat, a job's lease is renewed by the lease the server granted at claim time, at most the worker's `lease`. A server whose `max_lease` is shorter than the worker's `lease` no longer leaves the worker thinking it holds a job the server has taken back.
- A handler that returns normally after its timeout, deadline or the worker's shutdown ended its context is reported as that timeout, deadline or release, not as done.
- Stopping an idle polling worker no longer waits for its long-poll claim (up to 20s) or a reconnect backoff to end.
- A worker refuses a `concurrency` or `lease` that is not positive.
- Jobs handed back while the worker shuts down are awaited before `run()` returns. A JVM `Error`, such as `OutOfMemoryError`, is no longer caught as a handler failure.
- A URL with a user, password, path or query is refused instead of having those parts silently dropped. Pass credentials with `token`.
- A callback signature time too large for a timestamp is refused as a bad signature instead of overflowing, and the claim wait is measured without `nanoTime` overflow.
- A heartbeat that times out is followed by the next one at once, as in the Go worker. Before, the next beat was skipped, so one slow `Extend` could lose every running job's lease.
- A job that ends while the work stream reconnects no longer gives the server one credit too many, which let the worker run more jobs than `concurrency`.

## [0.1.0-beta.1] - 2026-11-16

First release. Works with Runnel 0.0.4 or later.

- `RunnelClient` with suspend calls for Kotlin, and `BlockingClient` with the same calls for Java and plain code: enqueue one job, many jobs or a flow, enqueue and wait, get, wait, list, cancel, retry and stats.
- `Worker` that takes Kotlin suspend handlers or Java `BlockingHandler`s. Jobs are pushed over one gRPC stream, or claimed with long-polling calls. Heartbeats, a lease watchdog, timeouts and deadlines on the server's clock, and a graceful shutdown. `serve()` runs until SIGTERM; `start()` gives a `CompletableFuture` for Java.
- Outcomes `fail`, `retryIn`, `busy` and `pending`, with a poller per kind.
- `RunnelException` with the gRPC code, the Runnel reason and the retry delay.
- `verifyCallback` and `parseCallback` for signed callbacks.
- Typed gRPC stubs for every call in `runnel.v1`.
