package dev.kinoforge.runnel

import kotlinx.coroutines.runBlocking
import java.time.Duration

public class BlockingClient(
    public val client: RunnelClient,
) : AutoCloseable {
    public constructor(config: ClientConfig) : this(RunnelClient(config))

    public val namespace: String get() = client.namespace

    @JvmOverloads
    public fun enqueue(
        queue: String,
        kind: String,
        payload: Any? = null,
        options: JobOptions = JobOptions(),
    ): Inserted = runBlocking { client.enqueue(queue, kind, payload, options) }

    public fun enqueueMany(jobs: List<NewJob>): List<Inserted> = runBlocking { client.enqueueMany(jobs) }

    public fun enqueueFlow(steps: Map<String, FlowStep>): Map<String, Inserted> = runBlocking { client.enqueueFlow(steps) }

    @JvmOverloads
    public fun enqueueAndWait(
        queue: String,
        kind: String,
        payload: Any? = null,
        options: JobOptions = JobOptions(),
        waitTimeout: Duration? = null,
    ): Job = runBlocking { client.enqueueAndWait(queue, kind, payload, options, waitTimeout) }

    public fun get(id: String): Job = runBlocking { client.get(id) }

    @JvmOverloads
    public fun wait(
        id: String,
        timeout: Duration? = null,
    ): Job = runBlocking { client.wait(id, timeout) }

    @JvmOverloads
    public fun listJobs(filter: JobFilter = JobFilter()): JobPage = runBlocking { client.listJobs(filter) }

    public fun cancel(id: String): Job = runBlocking { client.cancel(id) }

    public fun retry(id: String): Job = runBlocking { client.retry(id) }

    public fun stats(): Map<String, Map<String, Long>> = runBlocking { client.stats() }

    override fun close(): Unit = client.close()
}
