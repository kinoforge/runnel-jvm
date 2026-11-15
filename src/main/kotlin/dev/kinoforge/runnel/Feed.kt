package dev.kinoforge.runnel

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.Logger
import runnel.v1.Types
import runnel.v1.Worker.ClaimRequest
import runnel.v1.Worker.WorkRequest
import runnel.v1.Worker.WorkStart
import java.time.Duration
import java.util.concurrent.CancellationException
import kotlin.time.toKotlinDuration

internal class FeedConfig(
    val client: RunnelClient,
    val worker: String,
    val queues: List<String>,
    val lease: Duration,
    val claimWait: Duration,
    val retryMin: Duration,
    val retryMax: Duration,
    val log: Logger,
    val free: () -> Int,
    val deliver: (Types.Job) -> Unit,
)

internal interface Feed {
    suspend fun run()

    fun grant(n: Int)

    fun stop()

    fun abort()
}

internal suspend fun pause(
    d: Duration,
    wake: CompletableDeferred<Unit>,
) {
    withTimeoutOrNull(d.toKotlinDuration()) { wake.await() }
}

internal class Pause(
    private val c: FeedConfig,
) {
    @Volatile private var next: Duration = c.retryMin

    fun reset() {
        next = c.retryMin
    }

    suspend fun wait(wake: CompletableDeferred<Unit>) {
        pause(next, wake)
        next = minOf(next.multipliedBy(2), c.retryMax)
    }
}

internal class StreamFeed(
    private val c: FeedConfig,
) : Feed {
    @Volatile private var out: Channel<WorkRequest>? = null

    @Volatile private var session: kotlinx.coroutines.Job? = null

    @Volatile private var stopped = false
    private val aborted = CompletableDeferred<Unit>()

    override suspend fun run(): Unit =
        coroutineScope {
            val backoff = Pause(c)
            while (!stopped) {
                val q = Channel<WorkRequest>(Channel.UNLIMITED)
                val start =
                    WorkStart
                        .newBuilder()
                        .setNamespace(c.client.namespace)
                        .setWorker(c.worker)
                        .addAllQueues(c.queues)
                        .setLease(c.lease.toProto())
                q.trySend(WorkRequest.newBuilder().setStart(start).build())
                out = q
                grant(c.free())
                val s =
                    launch {
                        try {
                            c.client.rpc.workers.work(q.consumeAsFlow()).collect { r ->
                                backoff.reset()
                                c.client.clock.observe(if (r.hasNow()) r.now else null)
                                if (r.hasJob()) c.deliver(r.job)
                            }
                        } catch (e: CancellationException) {
                            throw e
                        } catch (e: Exception) {
                            if (!stopped) c.log.error("runnel: work stream failed: {}", toRunnelException(e).toString())
                        }
                    }
                session = s
                if (aborted.isCompleted) s.cancel()
                s.join()
                session = null
                out = null
                q.close()
                if (!stopped) backoff.wait(aborted)
            }
        }

    override fun grant(n: Int) {
        if (n > 0) out?.trySend(WorkRequest.newBuilder().setCredit(n).build())
    }

    override fun stop() {
        stopped = true
        out?.close()
    }

    override fun abort() {
        stop()
        aborted.complete(Unit)
        session?.cancel()
    }
}

internal class PollFeed(
    private val c: FeedConfig,
) : Feed {
    @Volatile private var stopped = false

    @Volatile private var call: kotlinx.coroutines.Job? = null
    private val wake = Channel<Unit>(Channel.CONFLATED)
    private val aborted = CompletableDeferred<Unit>()

    override suspend fun run(): Unit =
        supervisorScope {
            val backoff = Pause(c)
            while (!stopped) {
                wake.tryReceive()
                val n = c.free()
                if (n <= 0) {
                    wake.receive()
                    continue
                }
                val req =
                    ClaimRequest
                        .newBuilder()
                        .setNamespace(c.client.namespace)
                        .setWorker(c.worker)
                        .addAllQueues(c.queues)
                        .setLimit(n)
                        .setLease(c.lease.toProto())
                        .setWait(c.claimWait.toProto())
                        .build()
                val pending =
                    async {
                        c.client.rpc.workers
                            .claim(req)
                    }
                call = pending
                if (aborted.isCompleted) pending.cancel()
                val res =
                    try {
                        pending.await()
                    } catch (e: CancellationException) {
                        currentCoroutineContext().ensureActive()
                        break
                    } catch (e: Exception) {
                        if (stopped) break
                        c.log.error("runnel: claim failed: {}", toRunnelException(e).toString())
                        backoff.wait(aborted)
                        continue
                    } finally {
                        call = null
                    }
                backoff.reset()
                c.client.clock.observe(if (res.hasNow()) res.now else null)
                res.jobsList.forEach(c.deliver)
            }
        }

    override fun grant(n: Int) {
        wake.trySend(Unit)
    }

    override fun stop() {
        stopped = true
        wake.trySend(Unit)
    }

    override fun abort() {
        stop()
        aborted.complete(Unit)
        call?.cancel()
    }
}
