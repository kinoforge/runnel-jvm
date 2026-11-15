package dev.kinoforge.runnel.contract

import dev.kinoforge.runnel.ErrorReason
import dev.kinoforge.runnel.FlowStep
import dev.kinoforge.runnel.Job
import dev.kinoforge.runnel.JobOptions
import dev.kinoforge.runnel.JobState
import dev.kinoforge.runnel.NewJob
import dev.kinoforge.runnel.RunnelException
import dev.kinoforge.runnel.busy
import dev.kinoforge.runnel.fail
import dev.kinoforge.runnel.pending
import dev.kinoforge.runnel.retryIn
import io.grpc.Status
import kotlinx.coroutines.delay
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import runnel.v1.Config.PutQueueRequest
import runnel.v1.Config.Queue
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

class JobsTest {
    private fun Job.map(): Map<*, *>? = payload<Map<*, *>>()

    @Test
    fun `C01 a worker completes a job with a result`() =
        contract { e ->
            val c = e.client()
            e.work(c) { handle("welcome") { j, _ -> mapOf("sent" to true, "to" to j.map()?.get("to")) } }
            val job = c.enqueueAndWait("q", "welcome", mapOf("to" to "ana"), waitTimeout = WAIT)
            assertEquals(JobState.COMPLETED, job.state)
            assertEquals(mapOf("sent" to true, "to" to "ana"), job.result<Map<*, *>>())
            assertEquals(mapOf("sent" to true, "to" to "ana"), c.get(job.id).result<Map<*, *>>())
        }

    @Test
    fun `C02 a job that throws once completes on attempt 2`() =
        contract { e ->
            val c = e.client()
            val calls = AtomicInteger()
            e.work(c) {
                handle("flaky") { _, _ ->
                    if (calls.incrementAndGet() == 1) throw IllegalStateException("boom")
                    "ok"
                }
            }
            val job = c.enqueueAndWait("q", "flaky", options = JobOptions().apply { backoff = fast() }, waitTimeout = WAIT)
            assertEquals(JobState.COMPLETED, job.state)
            assertEquals(2, job.attempt)
            assertEquals(listOf("boom"), job.errors.map { it.error })
        }

    @Test
    fun `C03 a job that always throws is dead after its attempts`() =
        contract { e ->
            val c = e.client()
            e.work(c) { handleBlocking("broken", { _, _ -> throw IllegalStateException("still broken") }) }
            val o =
                JobOptions().apply {
                    maxAttempts = 2
                    backoff = fast()
                }
            val job = c.enqueueAndWait("q", "broken", options = o, waitTimeout = WAIT)
            assertEquals(JobState.DEAD, job.state)
            assertEquals(2, job.attempt)
            assertEquals(2, job.errors.size)
        }

    @Test
    fun `C04 fail ends a job at once`() =
        contract { e ->
            val c = e.client()
            e.work(c) { handle("strict") { _, _ -> throw fail(IllegalArgumentException("bad input")) } }
            val job = c.enqueueAndWait("q", "strict", waitTimeout = WAIT)
            assertEquals(JobState.DEAD, job.state)
            assertEquals(1, job.attempt)
            assertEquals("bad input", job.errors[0].error)
        }

    @Test
    fun `C05 retryIn waits the delay before the next attempt`() =
        contract { e ->
            val c = e.client()
            val starts = CopyOnWriteArrayList<Long>()
            e.work(c) {
                handle("later") { _, _ ->
                    starts += System.currentTimeMillis()
                    if (starts.size == 1) throw retryIn(Duration.ofSeconds(2), "not yet")
                    null
                }
            }
            val job = c.enqueueAndWait("q", "later", waitTimeout = WAIT)
            assertEquals(JobState.COMPLETED, job.state)
            assertEquals(2, starts.size)
            assertTrue(starts[1] - starts[0] >= 2000, "second attempt ${starts[1] - starts[0]}ms after the first")
        }

    @Test
    fun `C06 busy tries again without using an attempt`() =
        contract { e ->
            val c = e.client()
            val calls = AtomicInteger()
            e.work(c) { handle("crowded") { _, _ -> if (calls.incrementAndGet() == 1) busy(Duration.ofMillis(300)) else "ran" } }
            val job = c.enqueueAndWait("q", "crowded", waitTimeout = WAIT)
            assertEquals(JobState.COMPLETED, job.state)
            assertEquals(2, calls.get())
            assertEquals(1, job.attempt)
            assertEquals(emptyList<Any>(), job.errors)
        }

    @Test
    fun `C07 pending parks the job and the poller finishes it`() =
        contract { e ->
            val c = e.client()
            val handles = CopyOnWriteArrayList<String>()
            e.work(c) {
                handle("render", { _, _ -> pending("task-81", Duration.ofMillis(300)) }) { j, _ ->
                    handles += j.handle
                    if (handles.size < 3) pending("", Duration.ofMillis(300)) else mapOf("url" to "https://cdn.example/v.mp4")
                }
            }
            val job = c.enqueueAndWait("q", "render", waitTimeout = WAIT)
            assertEquals(JobState.COMPLETED, job.state)
            assertEquals(mapOf("url" to "https://cdn.example/v.mp4"), job.result<Map<*, *>>())
            assertEquals(listOf("task-81", "task-81", "task-81"), handles)
            assertEquals(1, job.attempt)
        }

    @Test
    fun `C08 a cancelled job never reaches a worker`() =
        contract { e ->
            val c = e.client()
            val ins = c.enqueue("q", "noop")
            assertEquals(JobState.CANCELLED, c.cancel(ins.id).state)
            val calls = AtomicInteger()
            val w = e.work(c) { handle("noop") { _, _ -> calls.incrementAndGet() } }
            delay(1500)
            w.stop()
            assertEquals(0, calls.get())
            assertEquals(JobState.CANCELLED, c.get(ins.id).state)
        }

    @Test
    fun `C13 a full queue is refused with the server's retry delay`() =
        contract { e ->
            val c = e.client()
            val q = Queue.newBuilder().setName("full").setMaxQueued(1)
            c.rpc.config.putQueue(
                PutQueueRequest
                    .newBuilder()
                    .setNamespace(e.ns)
                    .setQueue(q)
                    .build(),
            )
            c.enqueue("full", "k")
            val err = raises<RunnelException> { c.enqueue("full", "k") }
            assertEquals(Status.Code.RESOURCE_EXHAUSTED, err.code)
            assertEquals(ErrorReason.QUEUE_FULL, err.reason)
            assertTrue((err.retryAfter ?: Duration.ZERO) > Duration.ZERO, "retry after ${err.retryAfter}")
        }

    @Test
    fun `C14 a unique key makes one job`() =
        contract { e ->
            val c = e.client()
            val res = (1..5).map { c.enqueue("q", "k") { uniqueKey = "invoice-7" } }
            assertEquals(1, res.map { it.id }.toSet().size)
            assertEquals(4, res.count { it.duplicate })
        }

    @Test
    fun `C15 a flow runs in order and passes results on`() =
        contract { e ->
            val c = e.client()
            val order = CopyOnWriteArrayList<String>()
            val seen = CopyOnWriteArrayList<String>()
            e.work(c, { concurrency = 4 }) {
                handle("step") { j, ctx ->
                    val name = j.map()?.get("name") as String
                    order += name
                    if (name == "d") seen += ctx.dependencies().mapNotNull { it.result<String>() }.sorted()
                    name.uppercase()
                }
            }

            fun step(name: String) = NewJob("q", "step", mapOf("name" to name))
            val ids =
                c.enqueueFlow(
                    mapOf(
                        "a" to FlowStep(step("a")),
                        "b" to FlowStep(step("b"), listOf("a")),
                        "c" to FlowStep(step("c"), listOf("a")),
                        "d" to FlowStep(step("d"), listOf("b", "c")),
                    ),
                )
            val d = c.wait(ids.getValue("d").id, WAIT)
            assertEquals(JobState.COMPLETED, d.state)
            assertEquals("a", order[0])
            assertEquals("d", order[3])
            assertEquals(listOf("B", "C"), seen)
        }

    @Test
    fun `C18 a key without the producer role cannot enqueue`() =
        contract { e ->
            val c = e.client(listOf("reader"))
            val err = raises<RunnelException> { c.enqueue("q", "k") }
            assertEquals(Status.Code.PERMISSION_DENIED, err.code)
        }
}
