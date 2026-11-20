@file:JvmName("Callbacks")

package dev.kinoforge.runnel

import tools.jackson.databind.DeserializationFeature
import tools.jackson.databind.MapperFeature
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.KotlinModule
import java.security.MessageDigest
import java.time.DateTimeException
import java.time.Duration
import java.time.Instant
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

public val DEFAULT_MAX_AGE: Duration = Duration.ofMinutes(5)

public data class CallbackJob(
    val id: String = "",
    val namespace: String = "",
    val queue: String = "",
    val kind: String = "",
    val state: JobState = JobState.UNKNOWN,
    val attempt: Int = 0,
    val result: Any? = null,
    val error: String = "",
)

public data class CallbackBody(
    val event: String = "",
    val delivery: Long = 0,
    val job: CallbackJob = CallbackJob(),
)

private val INT = Regex("^[+-]?\\d+$")

private val mapper: JsonMapper =
    JsonMapper
        .builder()
        .addModule(KotlinModule.Builder().build())
        .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .build()

private fun hmac(
    secret: String,
    t: Long,
    body: ByteArray,
): String {
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(SecretKeySpec(secret.toByteArray(), "HmacSHA256"))
    mac.update("$t.".toByteArray())
    return mac.doFinal(body).joinToString("") { "%02x".format(it) }
}

public fun sign(
    secret: String,
    t: Long,
    body: ByteArray,
): String = "t=$t,v1=${hmac(secret, t, body)}"

private fun parse(header: String): Pair<Long, List<String>> {
    var t = 0L
    val sigs = mutableListOf<String>()
    for (part in header.split(",")) {
        val key = part.trim().substringBefore("=")
        val value = part.trim().substringAfter("=", "")
        when (key) {
            "t" -> {
                t =
                    value.takeIf { INT.matches(it) }?.toLongOrNull() ?: throw SignatureException("runnel: bad callback signature: bad time")
            }

            "v1" -> {
                sigs += value
            }
        }
    }
    if (t == 0L || sigs.isEmpty()) throw SignatureException("runnel: bad callback signature: missing t or v1")
    return t to sigs
}

@JvmOverloads
public fun verifyCallback(
    header: String,
    body: ByteArray,
    secrets: List<String>,
    maxAge: Duration? = null,
    now: Instant? = null,
) {
    val limit = maxAge?.takeIf { !it.isZero && !it.isNegative } ?: DEFAULT_MAX_AGE
    val (t, sigs) = parse(header)
    val signedAt =
        try {
            Instant.ofEpochSecond(t)
        } catch (_: DateTimeException) {
            throw SignatureException("runnel: bad callback signature: bad time")
        }
    val age = Duration.between(signedAt, now ?: Instant.now())
    if (age.abs() > limit) {
        throw SignatureException("runnel: bad callback signature: signed ${age.seconds}s from now")
    }
    for (secret in secrets) {
        if (secret.isEmpty()) continue
        val want = hmac(secret, t, body).toByteArray()
        if (sigs.any { MessageDigest.isEqual(it.toByteArray(), want) }) return
    }
    throw SignatureException("runnel: bad callback signature: no secret matches")
}

@JvmOverloads
public fun parseCallback(
    header: String,
    body: ByteArray,
    secrets: List<String>,
    maxAge: Duration? = null,
    now: Instant? = null,
): CallbackBody {
    verifyCallback(header, body, secrets, maxAge, now)
    return mapper.readValue(body, CallbackBody::class.java)
}
