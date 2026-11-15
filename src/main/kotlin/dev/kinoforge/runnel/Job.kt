package dev.kinoforge.runnel

import runnel.v1.Types
import java.time.Duration
import java.time.Instant

public enum class JobState {
    UNKNOWN,
    BLOCKED,
    WAITING,
    RUNNING,
    POLLING,
    COMPLETED,
    DEAD,
    CANCELLED,
    EXPIRED,
    ;

    public val isFinal: Boolean get() = this == COMPLETED || this == DEAD || this == CANCELLED || this == EXPIRED

    internal fun toProto(): Types.State = if (this == UNKNOWN) Types.State.STATE_UNSPECIFIED else Types.State.valueOf("STATE_$name")

    internal companion object {
        fun of(s: Types.State): JobState = entries.firstOrNull { it != UNKNOWN && "STATE_${it.name}" == s.name } ?: UNKNOWN
    }
}

public class AttemptError internal constructor(
    public val attempt: Int,
    public val queue: String,
    public val kind: String,
    public val at: Instant?,
    public val error: String,
) {
    override fun toString(): String = "AttemptError(attempt=$attempt, error=$error)"
}

public class Job internal constructor(
    private val pb: Types.Job,
    @PublishedApi internal val codec: Codec,
) {
    public val id: String get() = pb.id
    public val namespace: String get() = pb.namespace
    public val queue: String get() = pb.queue
    public val kind: String get() = pb.kind
    public val state: JobState get() = JobState.of(pb.state)
    public val priority: Int get() = pb.priority
    public val group: String get() = pb.group
    public val pool: String get() = pb.pool
    public val attempt: Int get() = pb.attempt
    public val maxAttempts: Int get() = pb.maxAttempts
    public val version: Long get() = pb.version
    public val worker: String get() = pb.worker
    public val handle: String get() = pb.handle
    public val uniqueKey: String get() = pb.uniqueKey
    public val dependsOn: List<String> get() = pb.dependsOnList
    public val callbackUrl: String get() = pb.callbackUrl
    public val meta: Map<String, String> get() = pb.metaMap
    public val rawPayload: ByteArray get() = pb.payload.toByteArray()
    public val rawResult: ByteArray get() = pb.result.toByteArray()
    public val errors: List<AttemptError>
        get() = pb.errorsList.map { AttemptError(it.attempt, it.queue, it.kind, if (it.hasAt()) it.at.toInstant() else null, it.error) }
    public val runAt: Instant? get() = if (pb.hasRunAt()) pb.runAt.toInstant() else null
    public val startBy: Instant? get() = if (pb.hasStartBy()) pb.startBy.toInstant() else null
    public val deadline: Instant? get() = if (pb.hasDeadline()) pb.deadline.toInstant() else null
    public val timeout: Duration? get() = if (pb.hasTimeout()) pb.timeout.toJava() else null
    public val leaseUntil: Instant? get() = if (pb.hasLeaseUntil()) pb.leaseUntil.toInstant() else null
    public val createdAt: Instant? get() = if (pb.hasCreatedAt()) pb.createdAt.toInstant() else null
    public val startedAt: Instant? get() = if (pb.hasStartedAt()) pb.startedAt.toInstant() else null
    public val finishedAt: Instant? get() = if (pb.hasFinishedAt()) pb.finishedAt.toInstant() else null
    public val isFinal: Boolean get() = state.isFinal
    public val isPolling: Boolean get() = pb.handle.isNotEmpty()

    public fun <T> payload(type: Class<T>): T? = codec.value(rawPayload, type)

    public fun <T> result(type: Class<T>): T? = codec.value(rawResult, type)

    public inline fun <reified T> payload(): T? = payload(T::class.java)

    public inline fun <reified T> result(): T? = result(T::class.java)

    public fun toProto(): Types.Job = pb

    override fun toString(): String = "Job(id=$id, queue=$queue, kind=$kind, state=$state, attempt=$attempt)"
}
