package dev.kinoforge.runnel

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import java.time.Duration
import java.time.Instant

class CallbackTest {
    private val vectors: List<JsonNode> =
        JsonMapper
            .builder()
            .build()
            .readTree(javaClass.getResourceAsStream("/callback-signatures.json"))
            .get("vectors")
            .toList()

    @Test
    fun `has the shared vectors`() {
        assertTrue(vectors.size >= 10)
    }

    @TestFactory
    fun vectors(): List<DynamicTest> =
        vectors.map { v ->
            DynamicTest.dynamicTest(v.get("name").asString()) {
                val check = {
                    verifyCallback(
                        v.get("header").asString(),
                        v.get("body").asString().toByteArray(),
                        v.get("secrets").toList().map { it.asString() },
                        Duration.ofSeconds(v.get("max_age_seconds").asLong()),
                        Instant.ofEpochSecond(v.get("now").asLong()),
                    )
                }
                if (v.get("valid").asBoolean()) assertDoesNotThrow(check) else assertThrows<SignatureException>(check)
            }
        }

    @Test
    fun `parse gives the typed body`() {
        val body = """{"event":"job.completed","delivery":7,"job":{"id":"j1","state":"completed","result":{"ok":true},"extra":1}}"""
        val header = sign("s", 1_800_000_000, body.toByteArray())
        val got = parseCallback(header, body.toByteArray(), listOf("s"), now = Instant.ofEpochSecond(1_800_000_000))
        assertEquals(7, got.delivery)
        assertEquals(JobState.COMPLETED, got.job.state)
        assertEquals(mapOf("ok" to true), got.job.result)
    }

    @Test
    fun `an old signature is refused unless max age allows it`() {
        val body = "{}".toByteArray()
        val header = sign("s", 1_800_000_000, body)
        val later = Instant.ofEpochSecond(1_800_000_000 + 600)
        assertThrows<SignatureException> { verifyCallback(header, body, listOf("s"), now = later) }
        assertDoesNotThrow { verifyCallback(header, body, listOf("s"), Duration.ofMinutes(15), later) }
    }
}
