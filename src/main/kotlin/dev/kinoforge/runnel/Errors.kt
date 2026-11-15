package dev.kinoforge.runnel

import io.grpc.Metadata
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import java.time.Duration
import java.util.concurrent.CancellationException
import kotlin.math.roundToLong

public enum class ErrorReason(public val wire: String) {
    INVALID("invalid"),
    NOT_FOUND("not_found"),
    STALE("stale"),
    QUEUE_FULL("queue_full"),
    GROUP_FULL("group_full"),
    NO_SPACE("no_space"),
    DEPENDENCY("dependency"),
    CONFLICT("conflict"),
    FINAL("final"),
    NOT_FINAL("not_final"),
    ;

    internal companion object {
        fun of(wire: String?): ErrorReason? = entries.firstOrNull { it.wire == wire }
    }
}

public class RunnelException(
    message: String,
    public val code: Status.Code,
    public val reason: ErrorReason? = null,
    public val retryAfter: Duration? = null,
    cause: Throwable? = null,
) : RuntimeException(message, cause) {
    public val isBusy: Boolean get() = code == Status.Code.RESOURCE_EXHAUSTED

    override fun toString(): String = "RunnelException: $code: $message"
}

internal val RETRYABLE: Set<Status.Code> =
    setOf(Status.Code.UNAVAILABLE, Status.Code.UNKNOWN, Status.Code.INTERNAL, Status.Code.DEADLINE_EXCEEDED)

private val ERROR_KEY: Metadata.Key<String> = Metadata.Key.of("runnel-error", Metadata.ASCII_STRING_MARSHALLER)
private val RETRY_KEY: Metadata.Key<String> = Metadata.Key.of("retry-after", Metadata.ASCII_STRING_MARSHALLER)

internal fun toRunnelException(err: Throwable): RunnelException =
    when (err) {
        is RunnelException -> err
        is StatusException -> fromStatus(err.status, err.trailers, err)
        is StatusRuntimeException -> fromStatus(err.status, err.trailers, err)
        else -> RunnelException(err.message ?: err.javaClass.simpleName, Status.Code.UNKNOWN, cause = err)
    }

private fun fromStatus(status: Status, trailers: Metadata?, cause: Throwable): RunnelException {
    val retry = trailers?.get(RETRY_KEY)?.toDoubleOrNull()?.let { Duration.ofNanos((it * 1e9).roundToLong()) }
    return RunnelException(status.description ?: "", status.code, ErrorReason.of(trailers?.get(ERROR_KEY)), retry, cause)
}

public class ShutdownException : CancellationException("runnel: worker shutting down")

public class JobTimeoutException(public val timeout: Duration) : CancellationException("runnel: attempt timed out after ${timeout.text()}s")

public class DeadlineException : CancellationException("runnel: deadline passed")

public class LeaseLostException : CancellationException("runnel: lease lost")

public class SignatureException(message: String) : RuntimeException(message)
