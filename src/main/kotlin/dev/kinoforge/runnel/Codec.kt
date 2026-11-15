package dev.kinoforge.runnel

import tools.jackson.databind.ObjectMapper
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.KotlinModule

public interface Codec {
    public fun encode(value: Any): ByteArray

    public fun <T> decode(bytes: ByteArray, type: Class<T>): T
}

public class JacksonCodec
    @JvmOverloads
    constructor(
        private val mapper: ObjectMapper = defaultMapper(),
    ) : Codec {
        override fun encode(value: Any): ByteArray = mapper.writeValueAsBytes(value)

        override fun <T> decode(bytes: ByteArray, type: Class<T>): T = mapper.readValue(bytes, type)

        public companion object {
            @JvmStatic
            public fun defaultMapper(): JsonMapper = JsonMapper.builder().addModule(KotlinModule.Builder().build()).build()
        }
    }

internal fun Codec.bytes(value: Any?): ByteArray =
    when (value) {
        null -> ByteArray(0)
        is ByteArray -> value
        else -> encode(value)
    }

internal fun <T> Codec.value(bytes: ByteArray, type: Class<T>): T? = if (bytes.isEmpty()) null else decode(bytes, type)
