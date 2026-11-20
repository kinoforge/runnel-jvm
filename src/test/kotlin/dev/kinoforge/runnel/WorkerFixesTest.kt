package dev.kinoforge.runnel

import com.google.protobuf.Timestamp
import io.grpc.Server
import io.grpc.ServerBuilder
import io.grpc.Status
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import runnel.v1.Types
import runnel.v1.Worker.ClaimRequest
import runnel.v1.Worker.ClaimResponse
import runnel.v1.Worker.ExtendRequest
import runnel.v1.Worker.ExtendResponse
import runnel.v1.Worker.FinishRequest
import runnel.v1.Worker.FinishResponse
import runnel.v1.Worker.OutcomeKind
import runnel.v1.WorkerServiceGrpcKt
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.seconds

class WorkerFixesTest {
    private class Fake(
        jobs: List<Types.Job>,
        jobsAgain: List<Types.Job> = emptyList(),
        private val after: CompletableDeferred<Unit> = CompletableDeferred(Unit),
        private val grant: Duration? = null,
        private val beat: (Int) -> Unit = {},
    ) : WorkerServiceGrpcKt.WorkerServiceCoroutineImplBase() {
        val finished = CopyOnWriteArrayList<OutcomeKind>()
        val claimedAt = CopyOnWriteArrayList<Instant>()
        private val batches = ConcurrentLinkedQueue(listOf(jobs) + listOf(jobsAgain).filter { it.isNotEmpty() })
        private val claims = AtomicInteger()
        private val beats = AtomicInteger()

        override suspend fun claim(request: ClaimRequest): ClaimResponse {
            if (claims.incrementAndGet() > 1) after.await()
            val next = batches.poll() ?: awaitCancellation()
            val now = Instant.now()
            claimedAt += now
            val until = grant?.let { now.plus(it) }
            val jobs = if (until == null) next else next.map { it.toBuilder().setLeaseUntil(until.toTimestamp()).build() }
            return ClaimResponse.newBuilder().addAllJobs(jobs).build()
        }

        override suspend fun extend(request: ExtendRequest): ExtendResponse {
            beat(beats.incrementAndGet())
            return ExtendResponse.getDefaultInstance()
        }

        override suspend fun finish(request: FinishRequest): FinishResponse {
            finished += request.outcome.kind
            return FinishResponse.getDefaultInstance()
        }
    }

    private fun Instant.toTimestamp(): Timestamp =
        Timestamp
            .newBuilder()
            .setSeconds(epochSecond)
            .setNanos(nano)
            .build()

    private val servers = mutableListOf<Server>()
    private val clients = mutableListOf<RunnelClient>()

    private fun client(fake: Fake): RunnelClient {
        val server =
            ServerBuilder
                .forPort(0)
                .addService(fake)
                .build()
                .start()
        servers += server
        return RunnelClient("http://127.0.0.1:${server.port}", "ns").also { clients += it }
    }

    @AfterEach
    fun close() {
        clients.forEach { it.close() }
        servers.forEach { it.shutdownNow() }
    }

    private fun job(
        id: String,
        build: Types.Job.Builder.() -> Unit = {},
    ): Types.Job =
        Types.Job
            .newBuilder()
            .setId(id)
            .setKind("k")
            .setQueue("q")
            .apply(build)
            .build()

    private fun waitFor(
        what: String,
        ok: () -> Boolean,
    ) = runBlocking {
        withTimeout(6.seconds) {
            while (!ok()) delay(10)
        }
        assertTrue(ok(), what)
    }

    @Test
    fun `a heartbeat renews by the lease the server granted`() {
        val fake = Fake(listOf(job("j1")), grant = Duration.ofSeconds(1)) { n -> if (n > 1) throw Status.UNAVAILABLE.asException() }
        val lostAt = CopyOnWriteArrayList<Instant>()
        val worker =
            Worker(client(fake), listOf("q")) {
                mode = WorkMode.POLL
                lease = Duration.ofSeconds(3)
                shutdownTimeout = Duration.ofMillis(200)
            }
        worker.handle("k") { _, _ ->
            try {
                awaitCancellation()
            } catch (e: CancellationException) {
                if (e is LeaseLostException) lostAt += Instant.now()
                throw e
            }
        }
        val run = worker.start()
        waitFor("the lease to end") { lostAt.isNotEmpty() }
        worker.stop()
        run.join()
        val took = Duration.between(fake.claimedAt[0], lostAt[0])
        assertTrue(took < Duration.ofMillis(3600), "the job was lost $took after it was granted")
    }

    @Test
    fun `a handler that returns after its timeout is retried`() {
        val fake =
            Fake(
                listOf(
                    job("j1") {
                        timeout =
                            com.google.protobuf.Duration
                                .newBuilder()
                                .setNanos(100_000_000)
                                .build()
                    },
                ),
            )
        val worker =
            Worker(client(fake), listOf("q")) {
                mode = WorkMode.POLL
                shutdownTimeout = Duration.ofMillis(200)
            }
        worker.handleBlocking("k", { _, ctx ->
            val end = System.nanoTime() + 5_000_000_000
            while (!ctx.isCancelled && System.nanoTime() < end) Thread.sleep(10)
            "ignored"
        })
        val run = worker.start()
        waitFor("the job to finish") { fake.finished.isNotEmpty() }
        worker.stop()
        run.join()
        assertEquals(OutcomeKind.OUTCOME_KIND_RETRY, fake.finished[0])
    }

    @Test
    fun `a job handed out again ends the run that still held it`() {
        val started = CompletableDeferred<Unit>()
        val fake = Fake(listOf(job("j1") { version = 1 }), jobsAgain = listOf(job("j1") { version = 2 }), after = started)
        val calls = AtomicInteger()
        val reason = CopyOnWriteArrayList<Throwable?>()
        val worker =
            Worker(client(fake), listOf("q")) {
                mode = WorkMode.POLL
                shutdownTimeout = Duration.ofMillis(200)
            }
        worker.handle("k") { _, ctx ->
            if (calls.incrementAndGet() > 1) return@handle "second"
            started.complete(Unit)
            try {
                awaitCancellation()
            } finally {
                reason += ctx.reason
            }
        }
        val run = worker.start()
        waitFor("the second run to finish") { fake.finished.isNotEmpty() }
        waitFor("the first run to end") { reason.isNotEmpty() }
        worker.stop()
        run.join()
        assertTrue(reason[0] is LeaseLostException, "the first run ended with ${reason[0]}")
        assertEquals(listOf(OutcomeKind.OUTCOME_KIND_DONE), fake.finished.toList())
    }

    @Test
    fun `an idle poll worker stops at once`() {
        val worker =
            Worker(client(Fake(emptyList())), listOf("q")) {
                mode = WorkMode.POLL
                shutdownTimeout = Duration.ofSeconds(5)
            }
        worker.handle("k") { _, _ -> null }
        val run = worker.start()
        Thread.sleep(100)
        val stopped = System.nanoTime()
        worker.stop()
        run.join()
        val took = Duration.ofNanos(System.nanoTime() - stopped)
        assertTrue(took < Duration.ofSeconds(1), "stop took $took")
    }

    @Test
    fun `a concurrency or lease that is not positive is refused`() {
        val c = client(Fake(emptyList()))
        val bad: List<WorkerOptions.() -> Unit> = listOf({ concurrency = 0 }, { lease = Duration.ZERO }, { lease = Duration.ofSeconds(-1) })
        for (o in bad) {
            val worker = Worker(c, listOf("q"), o)
            worker.handle("k") { _, _ -> null }
            assertThrows(IllegalArgumentException::class.java) { runBlocking { worker.run() } }
        }
    }

    @Test
    fun `a url with parts the client cannot use is refused`() {
        for (url in listOf("https://user:pw@runnel.internal", "https://runnel.internal/base", "https://runnel.internal?ca=x.pem")) {
            assertThrows(IllegalArgumentException::class.java) { target(url) }
        }
        assertEquals("runnel.internal", target("https://runnel.internal:8700/").host)
    }

    @Test
    fun `a signature time that does not fit is refused, not overflowed`() {
        for (t in listOf(Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE / 1000 + 1)) {
            assertThrows(SignatureException::class.java) { verifyCallback("t=$t,v1=00", ByteArray(0), listOf("s")) }
        }
    }
}
