package dev.kinoforge.runnel.contract

import dev.kinoforge.runnel.JobState
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit

class ServeTest {
    @Test
    fun `serve works until SIGTERM, then finishes running jobs and stops`() =
        contract { e ->
            val c = e.client()
            val java =
                ProcessHandle
                    .current()
                    .info()
                    .command()
                    .orElse("java")
            val pb = ProcessBuilder(java, "-cp", System.getProperty("java.class.path"), "dev.kinoforge.runnel.contract.ServeMainKt")
            pb.environment() += mapOf("URL" to URL, "NS" to e.ns, "TOKEN" to e.token(listOf("worker")))
            pb.redirectError(ProcessBuilder.Redirect.DISCARD)
            val proc = pb.start()
            try {
                val out = proc.inputReader()
                assertEquals("ready", withContext(Dispatchers.IO) { out.readLine() })
                assertEquals("pong", c.enqueueAndWait("q", "ping", waitTimeout = WAIT).result<String>())
                val slow = c.enqueue("q", "slow").id
                until(ok = { it == JobState.RUNNING }) { c.get(slow).state }
                proc.destroy()
                val exited = withContext(Dispatchers.IO) { proc.waitFor(15, TimeUnit.SECONDS) }
                assertEquals(true, exited)
                assertEquals(143, proc.exitValue())
                val job = c.get(slow)
                assertEquals(JobState.COMPLETED, job.state)
                assertEquals("done", job.result<String>())
            } finally {
                proc.destroyForcibly()
            }
        }
}
