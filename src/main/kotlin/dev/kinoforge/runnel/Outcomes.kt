@file:JvmName("Outcomes")

package dev.kinoforge.runnel

import com.google.protobuf.ByteString
import runnel.v1.Worker.Outcome
import runnel.v1.Worker.OutcomeKind
import java.time.Duration

public class Control internal constructor(
    internal val outcome: Outcome,
    message: String,
) : RuntimeException(message, null, false, false)

internal fun message(err: Any?): String =
    when (err) {
        null -> ""
        is Throwable -> err.message ?: err.javaClass.simpleName
        else -> err.toString()
    }

private fun outcome(kind: OutcomeKind): Outcome.Builder = Outcome.newBuilder().setKind(kind)

public fun fail(err: Any?): Control {
    val m = message(err)
    return Control(outcome(OutcomeKind.OUTCOME_KIND_FAIL).setError(m).build(), m)
}

@JvmOverloads
public fun retryIn(
    delay: Duration,
    err: Any? = null,
): Control {
    val m = message(err)
    return Control(outcome(OutcomeKind.OUTCOME_KIND_RETRY).setError(m).setDelay(delay.toProto()).build(), m)
}

public fun busy(delay: Duration): Control = Control(outcome(OutcomeKind.OUTCOME_KIND_BUSY).setDelay(delay.toProto()).build(), "busy")

public fun pending(
    handle: String,
    delay: Duration,
): Control = Control(outcome(OutcomeKind.OUTCOME_KIND_PENDING).setHandle(handle).setDelay(delay.toProto()).build(), "pending")

internal fun done(result: ByteArray): Outcome = outcome(OutcomeKind.OUTCOME_KIND_DONE).setResult(ByteString.copyFrom(result)).build()

internal fun retry(err: Any?): Outcome = outcome(OutcomeKind.OUTCOME_KIND_RETRY).setError(message(err).ifEmpty { "error" }).build()

internal fun release(): Outcome = outcome(OutcomeKind.OUTCOME_KIND_RELEASE).build()
