package dev.kinoforge.runnel

import io.grpc.Metadata
import io.grpc.Status
import io.grpc.StatusException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Duration

class ErrorsTest {
    private fun trailers(vararg pairs: Pair<String, String>): Metadata =
        Metadata().apply { pairs.forEach { put(Metadata.Key.of(it.first, Metadata.ASCII_STRING_MARSHALLER), it.second) } }

    @Test
    fun `a full queue carries the reason and the retry delay`() {
        val e =
            StatusException(
                Status.RESOURCE_EXHAUSTED.withDescription("queue full"),
                trailers(
                    "runnel-error" to "queue_full",
                    "retry-after" to "1.5",
                ),
            )
        val err = toRunnelException(e)
        assertEquals(Status.Code.RESOURCE_EXHAUSTED, err.code)
        assertEquals(ErrorReason.QUEUE_FULL, err.reason)
        assertEquals(Duration.ofMillis(1500), err.retryAfter)
        assertEquals("queue full", err.message)
        assertTrue(err.isBusy)
        assertSame(e, err.cause)
    }

    @Test
    fun `an error without trailers has no reason`() {
        val err = toRunnelException(Status.NOT_FOUND.asRuntimeException())
        assertEquals(Status.Code.NOT_FOUND, err.code)
        assertNull(err.reason)
        assertNull(err.retryAfter)
        assertFalse(err.isBusy)
    }

    @Test
    fun `an unknown reason and a bad retry delay are dropped`() {
        val err =
            toRunnelException(StatusException(Status.INVALID_ARGUMENT, trailers("runnel-error" to "new_thing", "retry-after" to "soon")))
        assertNull(err.reason)
        assertNull(err.retryAfter)
    }

    @Test
    fun `other errors are unknown`() {
        val err = toRunnelException(IllegalStateException("boom"))
        assertEquals(Status.Code.UNKNOWN, err.code)
        assertEquals("boom", err.message)
    }

    @Test
    fun `urls map to targets`() {
        assertEquals(Target("runnel.internal", 8700, false), target("http://runnel.internal:8700"))
        assertEquals(Target("runnel.example", 443, true), target("https://runnel.example"))
        assertEquals(Target("runnel.example", 80, false), target("http://runnel.example/"))
        assertEquals(Target("::1", 8700, false), target("http://[::1]:8700"))
        assertThrows<IllegalArgumentException> { target("grpc://x:1") }
        assertThrows<IllegalArgumentException> { target("http://") }
        assertThrows<IllegalArgumentException> { target("not a url") }
    }
}
