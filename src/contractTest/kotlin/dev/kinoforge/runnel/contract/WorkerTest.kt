package dev.kinoforge.runnel.contract

import com.sun.net.httpserver.HttpServer
import dev.kinoforge.runnel.CallbackBody
import dev.kinoforge.runnel.ClientConfig
import dev.kinoforge.runnel.DeadlineException
import dev.kinoforge.runnel.JobOptions
import dev.kinoforge.runnel.JobState
import dev.kinoforge.runnel.LeaseLostException
import dev.kinoforge.runnel.NewJob
import dev.kinoforge.runnel.RunnelClient
import dev.kinoforge.runnel.ShutdownException
import dev.kinoforge.runnel.WorkMode
import dev.kinoforge.runnel.parseCallback
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.net.InetSocketAddress
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.abs

class WorkerTest {
    private fun deadlineCheck(
        blocking: Boolean = false,
        client: ClientConfig.() -> Unit = {},
    ) = contract { e ->
        val c = e.client(configure = client)
        val reason = AtomicReference<Throwable?>()
        val at = AtomicReference<Long>()
        e.work(c) {
            if (blocking) {
                handleBlocking("slow", { _, ctx ->
                    try {
                        Thread.sleep(60_000)
                    } finally {
                        reason.set(ctx.reason)
                        at.set(System.currentTimeMillis())
                    }
                })
            } else {
                handle("slow") { _, ctx ->
                    try {
                        awaitCancellation()
                    } finally {
                        reason.set(ctx.reason)
                        at.set(System.currentTimeMillis())
                    }
                }
            }
        }
        val deadline = Instant.now().plusSeconds(2)
        val ins = c.enqueue("q", "slow", options = JobOptions().apply { this.deadline = deadline })
        val job = c.wait(ins.id, WAIT)
        assertEquals(JobState.EXPIRED, job.state)
        assertInstanceOf(DeadlineException::class.java, reason.get())
        val off = abs(at.get() - deadline.toEpochMilli())
        assertTrue(off < 500, "handler stopped ${off}ms from the deadline")
    }

    @Test
    fun `C09 the handler is cancelled at the deadline and the job expires`() = deadlineCheck()

    @Test
    fun `C09 a blocking handler is interrupted at the deadline`() = deadlineCheck(blocking = true)

    @Test
    fun `C19 deadlines use the server's clock when the worker's clock is off`() =
        deadlineCheck { clock = Clock.offset(Clock.systemUTC(), Duration.ofSeconds(30)) }

    @Test
    fun `C17 a signed callback arrives once and verifies`() {
        val secret = "whsec_contract"
        val got = CopyOnWriteArrayList<CallbackBody>()
        val srv = HttpServer.create(InetSocketAddress("0.0.0.0", 0), 0)
        srv.createContext("/hook") { x ->
            val body = x.requestBody.readAllBytes()
            got += parseCallback(x.requestHeaders.getFirst("Runnel-Signature") ?: "", body, listOf(secret))
            x.sendResponseHeaders(204, -1)
            x.close()
        }
        srv.start()
        try {
            contract(callbackSecret = secret) { e ->
                val c = e.client()
                e.work(c) { handle("notify") { _, _ -> mapOf("ok" to true) } }
                val url = "http://$CALLBACK_HOST:${srv.address.port}/hook"
                val ins = c.enqueue("q", "notify", options = JobOptions().apply { callbackUrl = url })
                until(ok = { it > 0 }) { got.size }
                delay(1500)
                assertEquals(1, got.size)
                assertEquals(ins.id, got[0].job.id)
                assertEquals(JobState.COMPLETED, got[0].job.state)
            }
        } finally {
            srv.stop(0)
        }
    }

    @ParameterizedTest
    @EnumSource(WorkMode::class)
    fun `C10 heartbeats keep a long job`(mode: WorkMode) =
        contract { e ->
            val c = e.client()
            val runs = AtomicInteger()
            val options: dev.kinoforge.runnel.WorkerOptions.() -> Unit = {
                this.mode = mode
                lease = Duration.ofMillis(1500)
            }
            val long: dev.kinoforge.runnel.Worker.() -> Unit = {
                handle("long") { _, _ ->
                    runs.incrementAndGet()
                    delay(4000)
                    "done"
                }
            }
            e.work(c, options, long)
            e.work(e.client(), options, long)
            val job = c.enqueueAndWait("q", "long", waitTimeout = WAIT)
            assertEquals(JobState.COMPLETED, job.state)
            assertEquals(1, job.attempt)
            assertEquals(emptyList<Any>(), job.errors)
            assertEquals(1, runs.get())
        }

    @ParameterizedTest
    @EnumSource(WorkMode::class)
    fun `C11 a lease that runs out is reported lost and the job runs again`(mode: WorkMode) =
        contract { e ->
            val key = e.token(listOf("producer", "reader", "worker"))
            val c =
                RunnelClient(URL, e.ns) {
                    token = key
                    interceptors = listOf(Dropping("Extend"))
                }
            e.clients += c
            val reason = AtomicReference<Throwable?>()
            val calls = AtomicInteger()
            e.work(c, {
                this.mode = mode
                lease = Duration.ofMillis(1500)
            }) {
                handle("held") { _, ctx ->
                    if (calls.incrementAndGet() > 1) return@handle "second"
                    try {
                        awaitCancellation()
                    } finally {
                        reason.set(ctx.reason)
                    }
                }
            }
            val job = c.enqueueAndWait("q", "held", options = JobOptions().apply { backoff = fast() }, waitTimeout = WAIT)
            assertInstanceOf(LeaseLostException::class.java, reason.get())
            assertEquals(JobState.COMPLETED, job.state)
            assertEquals(2, job.attempt)
            assertEquals(1, job.errors.size)
        }

    @ParameterizedTest
    @EnumSource(WorkMode::class)
    fun `C12 shutdown gives back held jobs untouched`(mode: WorkMode) =
        contract { e ->
            val c = e.client()
            val reason = AtomicReference<Throwable?>()
            val w =
                e.work(c, {
                    this.mode = mode
                    concurrency = 1
                    shutdownTimeout = Duration.ofMillis(300)
                }) {
                    handle("block") { _, ctx ->
                        try {
                            awaitCancellation()
                        } finally {
                            reason.set(ctx.reason)
                        }
                    }
                }
            val ids = c.enqueueMany(List(3) { NewJob("q", "block") }).map { it.id }
            until(ok = { it == 1 }) { w.worker.runningCount }
            w.stop()
            assertInstanceOf(ShutdownException::class.java, reason.get())
            for (id in ids) {
                val j = c.get(id)
                assertEquals(JobState.WAITING, j.state)
                assertEquals(0, j.attempt)
            }
        }

    @ParameterizedTest
    @EnumSource(WorkMode::class)
    fun `C16 concurrency caps jobs running at once`(mode: WorkMode) =
        contract { e ->
            val c = e.client()
            val now = AtomicInteger()
            val most = AtomicInteger()
            e.work(c, {
                this.mode = mode
                concurrency = 2
            }) {
                handle("task") { _, _ ->
                    most.accumulateAndGet(now.incrementAndGet(), ::maxOf)
                    delay(300)
                    now.decrementAndGet()
                }
            }
            val ids = c.enqueueMany(List(6) { NewJob("q", "task") }).map { it.id }
            for (id in ids) assertEquals(JobState.COMPLETED, c.wait(id, WAIT).state)
            assertEquals(2, most.get())
        }
}
