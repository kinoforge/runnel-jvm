# Changelog

## [0.1.0-beta.1] - 2026-11-16

First release. Works with Runnel 0.0.4 or later.

- `RunnelClient` with suspend calls for Kotlin, and `BlockingClient` with the same calls for Java and plain code: enqueue one job, many jobs or a flow, enqueue and wait, get, wait, list, cancel, retry and stats.
- `Worker` that takes Kotlin suspend handlers or Java `BlockingHandler`s. Jobs are pushed over one gRPC stream, or claimed with long-polling calls. Heartbeats, a lease watchdog, timeouts and deadlines on the server's clock, and a graceful shutdown. `serve()` runs until SIGTERM; `start()` gives a `CompletableFuture` for Java.
- Outcomes `fail`, `retryIn`, `busy` and `pending`, with a poller per kind.
- `RunnelException` with the gRPC code, the Runnel reason and the retry delay.
- `verifyCallback` and `parseCallback` for signed callbacks.
- Typed gRPC stubs for every call in `runnel.v1`.
