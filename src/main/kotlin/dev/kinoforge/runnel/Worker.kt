package dev.kinoforge.runnel

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.future.future
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.withTimeoutOrNull
import runnel.v1.Types
import runnel.v1.Worker.DependenciesRequest
import runnel.v1.Worker.ExtendRequest
import runnel.v1.Worker.FinishRequest
import runnel.v1.Worker.Outcome
import java.time.Duration
import java.util.concurrent.CancellationException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.toKotlinDuration

public class Worker(
    public val client: RunnelClient,
    public val queues: List<String>,
    public val options: WorkerOptions = WorkerOptions(),
) {
    public constructor(client: RunnelClient, queues: List<String>, configure: WorkerOptions.() -> Unit) :
        this(client, queues, WorkerOptions().apply(configure))

    public val id: String get() = options.id
    private val log get() = options.logger
    private val handlers = ConcurrentHashMap<String, Handlers>()
    private val running = ConcurrentHashMap<String, Run>()
    private val started = AtomicBoolean(false)
    private val stopping = AtomicBoolean(false)
    private val stopSignal = CompletableDeferred<Unit>()

    @Volatile private var feed: Feed? = null

    @Volatile private var scope: CoroutineScope? = null

    public val runningCount: Int get() = running.size

    @JvmSynthetic
    public fun handle(
        kind: String,
        run: Handler,
    ): Worker = add(kind, Handlers(run, null))

    @JvmSynthetic
    public fun handle(
        kind: String,
        run: Handler,
        poll: Handler,
    ): Worker = add(kind, Handlers(run, poll))

    @JvmOverloads
    public fun handleBlocking(
        kind: String,
        run: BlockingHandler,
        poll: BlockingHandler? = null,
    ): Worker = add(kind, Handlers(blocking(run), poll?.let(::blocking)))

    private fun blocking(h: BlockingHandler): Handler = { job, ctx -> runInterruptible(options.blockingDispatcher) { h.handle(job, ctx) } }

    private fun add(
        kind: String,
        h: Handlers,
    ): Worker {
        handlers[kind] = h
        return this
    }

    public fun stop() {
        stopSignal.complete(Unit)
    }

    public fun serve() {
        val closed = CountDownLatch(1)
        val hook =
            Thread({
                stop()
                closed.await()
            }, "runnel-shutdown")
        Runtime.getRuntime().addShutdownHook(hook)
        try {
            runBlocking(Dispatchers.Default) { run() }
        } finally {
            client.close()
            closed.countDown()
            runCatching { Runtime.getRuntime().removeShutdownHook(hook) }
        }
    }

    public fun start(): CompletableFuture<Void?> =
        CoroutineScope(Dispatchers.Default).future<Void?> {
            run()
            null
        }

    public suspend fun run() {
        require(queues.isNotEmpty() && handlers.isNotEmpty()) { "runnel: a worker needs queues and handlers" }
        check(started.compareAndSet(false, true)) { "runnel: worker is already running" }
        supervisorScope {
            scope = this
            work(this)
        }
    }

    private suspend fun work(s: CoroutineScope) {
        val config =
            FeedConfig(
                client = client,
                worker = id,
                queues = queues,
                lease = options.lease,
                claimWait = options.claimWait,
                retryMin = options.retryMin,
                retryMax = options.retryMax,
                log = log,
                free = { if (stopping.get()) 0 else options.concurrency - running.size },
                deliver = ::deliver,
            )
        val f = if (options.mode == WorkMode.POLL) PollFeed(config) else StreamFeed(config)
        feed = f
        val beats = s.launch { beats() }
        val fed = s.launch { f.run() }
        try {
            stopSignal.await()
            stopping.set(true)
            f.stop()
            if (!within(options.shutdownTimeout) { drained() }) {
                running.values.forEach { it.cancel(ShutdownException()) }
                drained()
            }
            if (!within(options.shutdownTimeout) { fed.join() }) f.abort()
            fed.join()
            drained()
        } finally {
            f.abort()
            fed.cancel()
            beats.cancel()
        }
    }

    private suspend fun within(
        d: Duration,
        block: suspend () -> Unit,
    ): Boolean = withTimeoutOrNull(d.toKotlinDuration()) { block() } != null

    private suspend fun drained() {
        running.values.mapNotNull { it.done }.joinAll()
    }

    private fun spawn(block: suspend () -> Unit): kotlinx.coroutines.Job {
        val s = scope ?: error("runnel: worker is not running")
        return s.launch(start = CoroutineStart.LAZY) { block() }
    }

    private fun deliver(pb: Types.Job) {
        if (stopping.get()) {
            spawn { finish(pb, release()) }.start()
            return
        }
        val now = client.clock.now()
        val until = if (pb.hasLeaseUntil()) pb.leaseUntil.toInstant() else now.plus(options.lease)
        val r = Run(pb, Job(pb, client.codec), Duration.between(now, until), until)
        running[pb.id] = r
        val t = spawn { execute(r) }
        r.done = t
        t.start()
    }

    private fun timers(
        r: Run,
        s: CoroutineScope,
    ): List<kotlinx.coroutines.Job> {
        val out = mutableListOf<kotlinx.coroutines.Job>()
        r.job.timeout?.takeIf { !it.isZero && !it.isNegative }?.let { t ->
            out +=
                s.launch {
                    delay(t.toKotlinDuration())
                    r.cancel(JobTimeoutException(t))
                }
        }
        r.job.deadline?.let { d ->
            val left = Duration.between(client.clock.now(), d)
            out +=
                s.launch {
                    delay(left.coerceAtLeast(Duration.ZERO).toKotlinDuration())
                    r.cancel(DeadlineException())
                }
        }
        return out
    }

    private suspend fun execute(r: Run) {
        try {
            val outcome =
                coroutineScope {
                    val timers = timers(r, this)
                    try {
                        attempt(r)
                    } finally {
                        timers.forEach { it.cancel() }
                    }
                }
            if (r.lost) {
                log.warn("runnel: lease lost, result dropped: {}", r.pb.id)
                return
            }
            finish(r.pb, outcome)
        } finally {
            val f = feed
            if (f == null) running.remove(r.pb.id, r) else f.freed { running.remove(r.pb.id, r) }
        }
    }

    private suspend fun attempt(r: Run): Outcome {
        val h = handlers[r.job.kind] ?: return fail("runnel: no handler for kind \"${r.job.kind}\"").outcome
        val fn = (if (r.job.isPolling) h.poll else h.run) ?: return fail("kind \"${r.job.kind}\" has a handle but no poller").outcome
        val ctx = JobContext(this, r)
        val value =
            supervisorScope {
                val t = async { fn(r.job, ctx) }
                r.attach(t)
                try {
                    Result.success(t.await())
                } catch (e: Throwable) {
                    currentCoroutineContext().ensureActive()
                    Result.failure(e)
                }
            }
        return value.fold(
            onSuccess = { v -> if (v is Control) v.outcome else runCatching { done(client.codec.bytes(v)) }.getOrElse(::retry) },
            onFailure = { e -> classify(e, r.reason) },
        )
    }

    private fun classify(
        err: Throwable,
        reason: CancellationException?,
    ): Outcome =
        when (reason) {
            is ShutdownException -> release()
            is JobTimeoutException -> retry("timed out after ${reason.timeout.text()}s")
            is DeadlineException -> fail("deadline passed").outcome
            else -> if (err is Control) err.outcome else retry(err)
        }

    private suspend fun finish(
        pb: Types.Job,
        outcome: Outcome,
    ) {
        var wait = options.retryMin
        val req =
            FinishRequest
                .newBuilder()
                .setNamespace(client.namespace)
                .setWorker(id)
                .setId(pb.id)
                .setVersion(pb.version)
                .setOutcome(outcome)
                .build()
        for (i in 1..options.finishTries) {
            try {
                client.rpc.workers.finish(req)
                return
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                val err = toRunnelException(e)
                if (err.code !in RETRYABLE) {
                    log.warn("runnel: finish refused, result dropped: {}: {}", pb.id, err.toString())
                    return
                }
                log.error("runnel: finishing job failed: {} try {}: {}", pb.id, i, err.toString())
                delay(wait.toKotlinDuration())
                wait = minOf(wait.multipliedBy(2), options.retryMax)
            }
        }
    }

    public suspend fun dependencies(job: Job): List<Job> {
        if (job.dependsOn.isEmpty()) return emptyList()
        val req =
            DependenciesRequest
                .newBuilder()
                .setNamespace(client.namespace)
                .addAllIds(job.dependsOn)
                .build()
        return call { client.rpc.workers.dependencies(req) }.jobsList.map { Job(it, client.codec) }
    }

    private suspend fun beats() {
        val every = options.lease.dividedBy(3).toNanos()
        var due = System.nanoTime() + every
        while (true) {
            delay(Duration.ofNanos(maxOf(0L, due - System.nanoTime())).toKotlinDuration())
            heartbeat()
            due = maxOf(due + every, System.nanoTime())
        }
    }

    private suspend fun heartbeat() {
        val sent = HashMap(running)
        if (sent.isEmpty()) return
        val third = options.lease.dividedBy(3)
        try {
            val req =
                ExtendRequest
                    .newBuilder()
                    .setNamespace(client.namespace)
                    .setWorker(id)
                    .addAllIds(sent.keys)
                    .setLease(options.lease.toProto())
                    .build()
            val res =
                client.rpc.workers
                    .withDeadlineAfter(third.toMillis(), TimeUnit.MILLISECONDS)
                    .extend(req)
            client.clock.observe(if (res.hasNow()) res.now else null)
            val now = client.clock.now()
            val lost = res.lostList.toSet()
            for ((id, r) in sent) {
                if (id in lost) lose(id, r) else r.leaseUntil = now.plus(r.lease)
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            log.error("runnel: heartbeat failed: {}", toRunnelException(e).toString())
        }
        val now = client.clock.now()
        for ((id, r) in sent) {
            if (!now.isBefore(r.leaseUntil)) lose(id, r)
        }
    }

    private fun lose(
        id: String,
        r: Run,
    ) {
        if (running[id] === r && r.markLost()) r.cancel(LeaseLostException())
    }
}
