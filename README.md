# Runnel for Kotlin and Java

The JVM client and worker for [Runnel](https://github.com/kinoforge/runnel), the job queue. It talks to `runnel serve` over gRPC, so workers get jobs pushed to them as soon as they have room.

Needs Java 17 or later and Runnel 0.0.4 or later. Kotlin code gets suspend calls; Java code gets blocking calls and a `CompletableFuture` for the worker.

```kotlin
implementation("dev.kinoforge:runnel:0.1.0-beta.1")
```

Until the first release is on Maven Central, take it from GitHub through [JitPack](https://jitpack.io/#kinoforge/runnel-jvm):

```kotlin
repositories {
    mavenCentral()
    maven("https://jitpack.io")
}

dependencies {
    implementation("com.github.kinoforge:runnel-jvm:main-SNAPSHOT")
}
```

Or build it yourself: `./gradlew publishToMavenLocal`, then add `mavenLocal()` to your repositories.

## Enqueue a job

From Kotlin:

```kotlin
val runnel = RunnelClient("http://runnel.internal:8700", "shop") { token = System.getenv("RUNNEL_KEY") }

val ins = runnel.enqueue("emails", "welcome", mapOf("to" to "ana@example.com"))
val job = runnel.enqueueAndWait("emails", "welcome", mapOf("to" to "ana@example.com"), waitTimeout = Duration.ofSeconds(30))
println("${job.state} ${job.result<Map<String, Any>>()}")

runnel.enqueue("emails", "welcome", Mail("ana@example.com")) {
    maxAttempts = 5
    delay = Duration.ofMinutes(1)
}
```

From Java, `BlockingClient` has the same calls:

```java
var config = new ClientConfig("http://runnel.internal:8700", "shop");
config.setToken(System.getenv("RUNNEL_KEY"));
var runnel = new BlockingClient(config);

var options = new JobOptions();
options.setMaxAttempts(5);
runnel.enqueue("emails", "welcome", Map.of("to", "ana@example.com"), options);
```

The key needs the `producer` role to enqueue, `reader` to read jobs and `admin` to cancel or retry them. Payloads and results are JSON, written and read with Jackson; pass a `ByteArray` to send a payload as it is. Durations are `java.time.Duration` and times are `Instant`.

## Run a worker

```kotlin
val worker = Worker(RunnelClient("http://runnel.internal:8700", "shop") { token = key }, listOf("emails")) {
    concurrency = 10
}

worker.handle("welcome") { job, ctx ->
    mailer.send(job.payload<Mail>()!!.to)
    mapOf("sent" to true)
}

worker.serve()
```

From Java, handlers are plain functions:

```java
var worker = new Worker(runnel.getClient(), List.of("emails"), new WorkerOptions());
worker.handleBlocking("welcome", (job, ctx) -> {
    mailer.send((String) job.payload(Map.class).get("to"));
    return Map.of("sent", true);
});
worker.serve();
```

The key needs the `worker` role. What the handler returns becomes the job's result. If it throws, the job is retried after the backoff, and is `DEAD` once its attempts run out.

`serve()` runs until SIGINT or SIGTERM, then returns. To run it next to other work, call `start()`, which gives a `CompletableFuture`, or `run()` from a coroutine; call `stop()` to end it.

The worker renews the leases of its running jobs every third of `lease`. On shutdown it stops taking jobs, waits up to `shutdownTimeout` for running ones, then cancels them; a cancelled job is given back untouched, without using an attempt.

Under `serve()` and `start()`, suspend handlers run on `Dispatchers.Default`; under `run()`, on the caller's dispatcher. Blocking handlers run on `blockingDispatcher`, `Dispatchers.IO` by default; give it a virtual thread executor with `Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher()` if you like.

## Outcomes

Return or throw one of these to choose what happens next. From Java they are `Outcomes.fail(...)` and so on.

| Call | What happens |
|---|---|
| `fail(err)` | the job is `DEAD` now, with no more retries |
| `retryIn(delay, err)` | the job runs again after exactly `delay` |
| `busy(delay)` | the job runs again after `delay`, without using an attempt |
| `pending(handle, delay)` | the job waits for outside work; its poller runs after `delay` |

```kotlin
worker.handle("charge") { job, ctx ->
    val res = payments.charge(job.payload<Charge>()!!)
    when (res.status) {
        429 -> busy(res.retryAfter)
        400 -> throw fail(res.error)
        else -> res.receipt
    }
}
```

## Outside work that takes a while

Give the kind a handler that starts the work and a poller that checks on it. `job.handle` holds what the handler passed to `pending`:

```kotlin
worker.handle(
    "video.render",
    { job, _ -> pending(renderer.start(job.rawPayload).id, Duration.ofSeconds(10)) },
) { job, _ ->
    val task = renderer.status(job.handle)
    when {
        task.running -> pending("", Duration.ofSeconds(10))
        task.lost -> throw fail("renderer lost the task")
        else -> mapOf("url" to task.url)
    }
}
```

From Java: `worker.handleBlocking("video.render", start, check)`.

## Cancellation

A suspend handler is cancelled like any coroutine. A blocking handler is interrupted, so `Thread.sleep`, blocking IO and `InterruptedException` end it; code that never blocks should check `ctx.isCancelled`. Either way, `ctx.reason` says why:

| Reason | When |
|---|---|
| `JobTimeoutException` | the attempt ran longer than the job's `timeout`; the job is retried |
| `DeadlineException` | the job's `deadline` passed; the job is `EXPIRED` |
| `LeaseLostException` | the job was cancelled, or its lease ran out; the result is dropped |
| `ShutdownException` | the worker is stopping; the job is given back |

Deadlines and leases are measured on the server's clock. The worker learns it from every answer, so a worker whose own clock is off still stops on time.

## Errors

Calls throw `RunnelException` with the gRPC `code`, the Runnel `reason` and, when the server is full, `retryAfter`:

```kotlin
try {
    runnel.enqueue("emails", "welcome")
} catch (err: RunnelException) {
    if (err.reason == ErrorReason.QUEUE_FULL) delay((err.retryAfter ?: Duration.ofSeconds(1)).toKotlinDuration())
}
```

`reason` is one of `INVALID`, `NOT_FOUND`, `STALE`, `QUEUE_FULL`, `GROUP_FULL`, `NO_SPACE`, `DEPENDENCY`, `CONFLICT`, `FINAL` and `NOT_FINAL`.

## Callbacks

When a namespace has a callback secret, Runnel signs every callback. Check it before trusting the body:

```kotlin
val body = parseCallback(request.getHeader("Runnel-Signature") ?: "", rawBody, listOf(System.getenv("RUNNEL_CALLBACK_SECRET")))
println("${body.event} ${body.job.id} ${body.job.state}")
```

From Java: `Callbacks.parseCallback(header, rawBody, List.of(secret))`.

Pass the raw body bytes, not parsed JSON. Give two secrets while you rotate them. A signature older than 5 minutes is refused; pass `maxAge` to change that. A delivery can arrive twice: dedupe by `body.delivery`. Invalid signatures throw `SignatureException`.

## Options

`ClientConfig`:

| Option | Default | Meaning |
|---|---|---|
| `url` | | the server, `http://` or `https://` |
| `namespace` | | the namespace every call works in |
| `token` | | a namespace key `rnk_…`, or the admin token |
| `credentials` | system roots | `ChannelCredentials` for `https://`, such as your own CA or a client certificate |
| `channelBuilder` | | changes the gRPC channel builder, such as keepalive |
| `channel` | | a gRPC channel of your own; the client does not close it |
| `interceptors` | | gRPC client interceptors |
| `codec` | Jackson | how payloads and results become bytes |
| `clock` | system clock | the local clock |
| `maxWait` | `60s` | the longest single wait `wait` asks the server for |
| `closeTimeout` | `5s` | how long `close` waits for calls to finish |

`WorkerOptions`:

| Option | Default | Meaning |
|---|---|---|
| `concurrency` | `10` | jobs run at once |
| `lease` | `30s` | how long a job is held between heartbeats |
| `shutdownTimeout` | `30s` | how long a stopping worker waits for running jobs |
| `mode` | `STREAM` | `STREAM` has jobs pushed over one gRPC stream; `POLL` claims them with long-polling calls |
| `claimWait` | `20s` | in `POLL` mode, how long one claim waits for jobs |
| `id` | host, pid and a random suffix | the worker's name in leases |
| `finishTries` | `6` | tries to report an outcome when the server is unreachable |
| `retryMin`, `retryMax` | `200ms`, `5s` | the backoff between those tries and between reconnects |
| `blockingDispatcher` | `Dispatchers.IO` | where blocking handlers run |
| `logger` | SLF4J logger `runnel` | where errors and warnings go |

## Lower level

`client.rpc` holds coroutine stubs for every call of the [gRPC API](https://github.com/kinoforge/runnel/blob/main/docs/grpc-api.md), including settings and admin. From Java, make a stub on `client.getChannel()`, such as `AdminServiceGrpc.newBlockingStub(client.getChannel())`. The messages are in `runnel.v1`, such as `runnel.v1.Config.Queue`.

## Tests

`./gradlew test` runs the unit tests. `./gradlew contractTest` runs the [shared SDK scenarios](https://github.com/kinoforge/runnel/tree/main/sdk-contract) against a real server at `RUNNEL_URL`; CI starts one in Docker. `./gradlew ktlintCheck` runs the linter, and `scripts/sync-proto.sh` copies the protos of a Runnel version.

## License

Apache 2.0.
