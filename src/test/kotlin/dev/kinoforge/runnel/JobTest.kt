package dev.kinoforge.runnel

import com.google.protobuf.ByteString
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import runnel.v1.Types
import runnel.v1.Worker.OutcomeKind
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset

class JobTest {
    private val codec = JacksonCodec()

    data class Mail(
        val to: String,
    )

    @Test
    fun `every state maps both ways`() {
        for (s in JobState.entries.filter { it != JobState.UNKNOWN }) assertEquals(s, JobState.of(s.toProto()))
        assertEquals(JobState.UNKNOWN, JobState.of(Types.State.STATE_UNSPECIFIED))
        assertTrue(JobState.EXPIRED.isFinal)
        assertFalse(JobState.POLLING.isFinal)
    }

    @Test
    fun `a job reads its fields and decodes its payload`() {
        val at = Instant.parse("2026-11-15T10:00:00.250Z")
        val pb =
            Types.Job
                .newBuilder()
                .setId("j1")
                .setQueue("emails")
                .setKind("welcome")
                .setState(Types.State.STATE_RUNNING)
                .setPayload(ByteString.copyFromUtf8("""{"to":"ana"}"""))
                .setDeadline(at.toTimestamp())
                .setTimeout(Duration.ofMillis(1500).toProto())
                .addErrors(
                    Types.AttemptError
                        .newBuilder()
                        .setAttempt(1)
                        .setError("boom")
                        .setAt(at.toTimestamp()),
                ).build()
        val j = Job(pb, codec)
        assertEquals("j1", j.id)
        assertEquals(JobState.RUNNING, j.state)
        assertEquals(Mail("ana"), j.payload<Mail>())
        assertEquals(mapOf("to" to "ana"), j.payload<Map<*, *>>())
        assertNull(j.result<Any>())
        assertEquals(at, j.deadline)
        assertEquals(Duration.ofMillis(1500), j.timeout)
        assertNull(j.runAt)
        assertEquals("boom", j.errors.single().error)
        assertEquals(at, j.errors.single().at)
        assertFalse(j.isPolling)
    }

    @Test
    fun `a new job carries its options`() {
        val o =
            JobOptions().apply {
                maxAttempts = 5
                delay = Duration.ofSeconds(60)
                deadline = Instant.ofEpochSecond(1_800_000_000)
                backoff = Backoff(base = Duration.ofMillis(100), factor = 1.0)
                meta = mapOf("k" to "v")
            }
        val pb = NewJob("q", "k", mapOf("a" to 1), o).toProto(codec)
        assertEquals("""{"a":1}""", pb.payload.toStringUtf8())
        assertEquals(5, pb.maxAttempts)
        assertEquals(60, pb.delay.seconds)
        assertEquals(1_800_000_000, pb.deadline.seconds)
        assertEquals(100_000_000, pb.backoff.base.nanos)
        assertFalse(pb.backoff.hasMax())
        assertFalse(pb.hasTtl())
        assertEquals("v", pb.metaMap["k"])
    }

    @Test
    fun `bytes pass as they are and null is empty`() {
        val raw = byteArrayOf(1, 2, 3)
        assertArrayEquals(raw, codec.bytes(raw))
        assertEquals(0, codec.bytes(null).size)
    }

    @Test
    fun `outcomes carry their kind, delay and error`() {
        assertEquals(OutcomeKind.OUTCOME_KIND_FAIL, fail(IllegalArgumentException("bad input")).outcome.kind)
        assertEquals("bad input", fail(IllegalArgumentException("bad input")).outcome.error)
        val r = retryIn(Duration.ofSeconds(2), "not yet").outcome
        assertEquals(OutcomeKind.OUTCOME_KIND_RETRY, r.kind)
        assertEquals(2, r.delay.seconds)
        assertEquals("not yet", r.error)
        assertEquals(OutcomeKind.OUTCOME_KIND_BUSY, busy(Duration.ofMillis(300)).outcome.kind)
        assertEquals("task-81", pending("task-81", Duration.ofSeconds(1)).outcome.handle)
        assertEquals("error", retry(null).error)
        assertEquals("IllegalStateException", retry(IllegalStateException()).error)
        assertEquals(OutcomeKind.OUTCOME_KIND_RELEASE, release().kind)
    }

    @Test
    fun `the server clock follows the server`() {
        val local = Clock.fixed(Instant.ofEpochSecond(1000), ZoneOffset.UTC)
        val c = ServerClock(local)
        assertEquals(Instant.ofEpochSecond(1000), c.now())
        c.observe(Instant.ofEpochSecond(1030).toTimestamp())
        assertEquals(Instant.ofEpochSecond(1030), c.now())
        c.observe(null)
        c.observe(Types.Job.getDefaultInstance().createdAt)
        assertEquals(Instant.ofEpochSecond(1030), c.now())
    }

    @Test
    fun `durations print like seconds`() {
        assertEquals("2", Duration.ofSeconds(2).text())
        assertEquals("1.5", Duration.ofMillis(1500).text())
    }
}
