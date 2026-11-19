package dev.kinoforge.runnel

import io.grpc.Server
import io.grpc.ServerBuilder
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import runnel.v1.Types
import runnel.v1.Worker.ClaimRequest
import runnel.v1.Worker.ClaimResponse
import runnel.v1.Worker.ExtendRequest
import runnel.v1.Worker.ExtendResponse
import runnel.v1.Worker.FinishRequest
import runnel.v1.Worker.FinishResponse
import runnel.v1.WorkerServiceGrpcKt
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.seconds

class WorkerBeatTest {
    private class Fake : WorkerServiceGrpcKt.WorkerServiceCoroutineImplBase() {
        val extends = CopyOnWriteArrayList<Long>()
        val second = CompletableDeferred<Unit>()
        private val claims = AtomicInteger()

        override suspend fun claim(request: ClaimRequest): ClaimResponse {
            if (claims.getAndIncrement() > 0) awaitCancellation()
            return ClaimResponse
                .newBuilder()
                .addJobs(
                    Types.Job
                        .newBuilder()
                        .setId("j1")
                        .setKind("k")
                        .setQueue("q"),
                ).build()
        }

        override suspend fun extend(request: ExtendRequest): ExtendResponse {
            extends += System.nanoTime()
            if (extends.size == 1) awaitCancellation()
            second.complete(Unit)
            return ExtendResponse.getDefaultInstance()
        }

        override suspend fun finish(request: FinishRequest): FinishResponse = FinishResponse.getDefaultInstance()
    }

    private val fake = Fake()
    private val server: Server =
        ServerBuilder
            .forPort(0)
            .addService(fake)
            .build()
            .start()

    @AfterEach
    fun close() {
        server.shutdownNow()
    }

    @Test
    fun `a timed-out heartbeat is followed at once`() {
        val client = RunnelClient("http://127.0.0.1:${server.port}", "ns")
        val worker =
            Worker(client, listOf("q")) {
                mode = WorkMode.POLL
                lease = Duration.ofMillis(1500)
                shutdownTimeout = Duration.ofMillis(200)
            }
        worker.handle("k") { _, _ -> awaitCancellation() }
        val run = worker.start()
        runBlocking { withTimeout(5.seconds) { fake.second.await() } }
        worker.stop()
        run.join()
        client.close()
        val gap = Duration.ofNanos(fake.extends[1] - fake.extends[0])
        assertTrue(gap < Duration.ofMillis(750), "second heartbeat came $gap after the first")
    }
}
