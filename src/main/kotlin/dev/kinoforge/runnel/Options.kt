package dev.kinoforge.runnel

import com.google.protobuf.ByteString
import runnel.v1.Types
import java.time.Duration
import java.time.Instant

public class Backoff
    @JvmOverloads
    constructor(
        public var base: Duration? = null,
        public var max: Duration? = null,
        public var factor: Double = 0.0,
        public var jitter: Double = 0.0,
    ) {
        internal fun toProto(): Types.Backoff {
            val b =
                Types.Backoff
                    .newBuilder()
                    .setFactor(factor)
                    .setJitter(jitter)
            base?.let { b.base = it.toProto() }
            max?.let { b.max = it.toProto() }
            return b.build()
        }
    }

public class JobOptions {
    public var priority: Int = 0
    public var group: String = ""
    public var pool: String = ""
    public var runAt: Instant? = null
    public var delay: Duration? = null
    public var maxAttempts: Int = 0
    public var uniqueKey: String = ""
    public var uniqueFor: Duration? = null
    public var startTimeout: Duration? = null
    public var deadline: Instant? = null
    public var ttl: Duration? = null
    public var timeout: Duration? = null
    public var backoff: Backoff? = null
    public var dependsOn: List<String> = emptyList()
    public var callbackUrl: String = ""
    public var meta: Map<String, String> = emptyMap()
}

public class NewJob
    @JvmOverloads
    constructor(
        public val queue: String,
        public val kind: String,
        public val payload: Any? = null,
        public val options: JobOptions = JobOptions(),
    ) {
        internal fun toProto(codec: Codec): Types.NewJob {
            val o = options
            val b =
                Types.NewJob
                    .newBuilder()
                    .setQueue(queue)
                    .setKind(kind)
                    .setPayload(ByteString.copyFrom(codec.bytes(payload)))
                    .setPriority(o.priority)
                    .setGroup(o.group)
                    .setPool(o.pool)
                    .setMaxAttempts(o.maxAttempts)
                    .setUniqueKey(o.uniqueKey)
                    .addAllDependsOn(o.dependsOn)
                    .setCallbackUrl(o.callbackUrl)
                    .putAllMeta(o.meta)
            o.runAt?.let { b.runAt = it.toTimestamp() }
            o.deadline?.let { b.deadline = it.toTimestamp() }
            o.delay?.let { b.delay = it.toProto() }
            o.uniqueFor?.let { b.uniqueFor = it.toProto() }
            o.startTimeout?.let { b.startTimeout = it.toProto() }
            o.ttl?.let { b.ttl = it.toProto() }
            o.timeout?.let { b.timeout = it.toProto() }
            o.backoff?.let { b.backoff = it.toProto() }
            return b.build()
        }
    }

public class FlowStep
    @JvmOverloads
    constructor(
        public val job: NewJob,
        public val after: List<String> = emptyList(),
    )

public class JobFilter {
    public var queue: String = ""
    public var kind: String = ""
    public var group: String = ""
    public var state: JobState? = null
    public var cursor: String = ""
    public var limit: Int = 0
}

public data class Inserted(
    val id: String,
    val duplicate: Boolean,
    val error: String,
)

public class JobPage internal constructor(
    public val jobs: List<Job>,
    public val nextCursor: String,
)
