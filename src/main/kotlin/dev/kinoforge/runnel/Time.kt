package dev.kinoforge.runnel

import com.google.protobuf.Timestamp
import java.time.Clock
import java.time.Duration
import java.time.Instant
import com.google.protobuf.Duration as PbDuration

internal fun Timestamp.toInstant(): Instant = Instant.ofEpochSecond(seconds, nanos.toLong())

internal fun Instant.toTimestamp(): Timestamp =
    Timestamp
        .newBuilder()
        .setSeconds(epochSecond)
        .setNanos(nano)
        .build()

internal fun Duration.toProto(): PbDuration =
    PbDuration
        .newBuilder()
        .setSeconds(seconds)
        .setNanos(nano)
        .build()

internal fun PbDuration.toJava(): Duration = Duration.ofSeconds(seconds, nanos.toLong())

internal fun Duration.text(): String {
    val s = toNanos() / 1e9
    return if (s == Math.floor(s)) s.toLong().toString() else s.toString()
}

public class ServerClock(private val local: Clock) {
    @Volatile private var offset: Duration = Duration.ZERO

    internal fun observe(server: Timestamp?) {
        if (server == null || (server.seconds == 0L && server.nanos == 0)) return
        offset = Duration.between(local.instant(), server.toInstant())
    }

    public fun now(): Instant = local.instant().plus(offset)
}
