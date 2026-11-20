package dev.kinoforge.runnel

import io.grpc.Channel
import io.grpc.ChannelCredentials
import io.grpc.ClientInterceptor
import io.grpc.ClientInterceptors
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import runnel.v1.AdminServiceGrpcKt.AdminServiceCoroutineStub
import runnel.v1.ConfigServiceGrpcKt.ConfigServiceCoroutineStub
import runnel.v1.Job.CancelJobRequest
import runnel.v1.Job.EnqueueFlowRequest
import runnel.v1.Job.EnqueueRequest
import runnel.v1.Job.GetJobRequest
import runnel.v1.Job.GetStatsRequest
import runnel.v1.Job.ListJobsRequest
import runnel.v1.Job.RetryJobRequest
import runnel.v1.Job.WaitJobRequest
import runnel.v1.JobServiceGrpcKt.JobServiceCoroutineStub
import runnel.v1.Types
import runnel.v1.WorkerServiceGrpcKt.WorkerServiceCoroutineStub
import java.time.Clock
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.function.Consumer
import runnel.v1.Job.FlowStep as PbFlowStep

public class ClientConfig(
    public val url: String,
    public val namespace: String,
) {
    public var token: String? = null
    public var credentials: ChannelCredentials? = null
    public var channelBuilder: Consumer<ManagedChannelBuilder<*>>? = null
    public var channel: ManagedChannel? = null
    public var interceptors: List<ClientInterceptor> = emptyList()
    public var codec: Codec = JacksonCodec()
    public var clock: Clock = Clock.systemUTC()
    public var maxWait: Duration = Duration.ofSeconds(60)
    public var closeTimeout: Duration = Duration.ofSeconds(5)
}

public class Rpc internal constructor(
    channel: Channel,
) {
    public val jobs: JobServiceCoroutineStub = JobServiceCoroutineStub(channel)
    public val workers: WorkerServiceCoroutineStub = WorkerServiceCoroutineStub(channel)
    public val config: ConfigServiceCoroutineStub = ConfigServiceCoroutineStub(channel)
    public val admin: AdminServiceCoroutineStub = AdminServiceCoroutineStub(channel)
}

internal suspend inline fun <T> call(block: () -> T): T =
    try {
        block()
    } catch (e: StatusException) {
        throw toRunnelException(e)
    } catch (e: StatusRuntimeException) {
        throw toRunnelException(e)
    }

public class RunnelClient(
    public val config: ClientConfig,
) : AutoCloseable {
    @JvmOverloads
    public constructor(url: String, namespace: String, configure: ClientConfig.() -> Unit = {}) :
        this(ClientConfig(url, namespace).apply(configure))

    public val namespace: String get() = config.namespace
    public val clock: ServerClock = ServerClock(config.clock)
    internal val codec: Codec get() = config.codec
    private val base: ManagedChannel = config.channel ?: buildChannel(config)
    public val channel: Channel = ClientInterceptors.intercept(base, config.interceptors + listOfNotNull(config.token?.let(::Auth)))
    public val rpc: Rpc = Rpc(channel)

    public suspend fun enqueue(
        queue: String,
        kind: String,
        payload: Any? = null,
        options: JobOptions = JobOptions(),
    ): Inserted = enqueueMany(listOf(NewJob(queue, kind, payload, options))).firstOrNull() ?: error("runnel: enqueue answered no result")

    @JvmSynthetic
    public suspend fun enqueue(
        queue: String,
        kind: String,
        payload: Any? = null,
        configure: JobOptions.() -> Unit,
    ): Inserted = enqueue(queue, kind, payload, JobOptions().apply(configure))

    public suspend fun enqueueMany(jobs: List<NewJob>): List<Inserted> {
        val req =
            EnqueueRequest
                .newBuilder()
                .setNamespace(namespace)
                .addAllJobs(jobs.map { it.toProto(codec) })
                .build()
        return call { rpc.jobs.enqueue(req) }.resultsList.map(::inserted)
    }

    public suspend fun enqueueFlow(steps: Map<String, FlowStep>): Map<String, Inserted> {
        val req = EnqueueFlowRequest.newBuilder().setNamespace(namespace)
        for ((name, s) in steps) {
            req.addSteps(
                PbFlowStep
                    .newBuilder()
                    .setName(name)
                    .setJob(s.job.toProto(codec))
                    .addAllAfter(s.after),
            )
        }
        return call { rpc.jobs.enqueueFlow(req.build()) }.stepsMap.mapValues { inserted(it.value) }
    }

    public suspend fun enqueueAndWait(
        queue: String,
        kind: String,
        payload: Any? = null,
        options: JobOptions = JobOptions(),
        waitTimeout: Duration? = null,
    ): Job = wait(enqueue(queue, kind, payload, options).id, waitTimeout)

    public suspend fun get(id: String): Job =
        job(
            call {
                rpc.jobs.getJob(
                    GetJobRequest
                        .newBuilder()
                        .setNamespace(namespace)
                        .setId(id)
                        .build(),
                )
            }.let { if (it.hasJob()) it.job else null },
        )

    public suspend fun wait(
        id: String,
        timeout: Duration? = null,
    ): Job {
        val until = timeout?.let { System.nanoTime() + it.toNanos() }
        while (true) {
            val left = if (until == null) config.maxWait else minOf(Duration.ofNanos(until - System.nanoTime()), config.maxWait)
            val wait = if (left.isNegative) Duration.ZERO else left
            val req =
                WaitJobRequest
                    .newBuilder()
                    .setNamespace(namespace)
                    .setId(id)
                    .setWait(wait.toProto())
                    .build()
            val res = call { rpc.jobs.waitJob(req) }
            val j = job(if (res.hasJob()) res.job else null)
            if (res.finished || j.isFinal || (until != null && System.nanoTime() - until >= 0L)) return j
        }
    }

    public suspend fun listJobs(filter: JobFilter = JobFilter()): JobPage {
        val req =
            ListJobsRequest
                .newBuilder()
                .setNamespace(namespace)
                .setQueue(filter.queue)
                .setKind(filter.kind)
                .setGroup(filter.group)
                .setState(filter.state?.toProto() ?: Types.State.STATE_UNSPECIFIED)
                .setCursor(filter.cursor)
                .setLimit(filter.limit)
                .build()
        val res = call { rpc.jobs.listJobs(req) }
        return JobPage(res.jobsList.map { Job(it, codec) }, res.nextCursor)
    }

    @JvmSynthetic
    public suspend fun listJobs(configure: JobFilter.() -> Unit): JobPage = listJobs(JobFilter().apply(configure))

    public suspend fun cancel(id: String): Job =
        job(
            call {
                rpc.jobs.cancelJob(
                    CancelJobRequest
                        .newBuilder()
                        .setNamespace(namespace)
                        .setId(id)
                        .build(),
                )
            }.let { if (it.hasJob()) it.job else null },
        )

    public suspend fun retry(id: String): Job =
        job(
            call {
                rpc.jobs.retryJob(
                    RetryJobRequest
                        .newBuilder()
                        .setNamespace(namespace)
                        .setId(id)
                        .build(),
                )
            }.let { if (it.hasJob()) it.job else null },
        )

    public suspend fun stats(): Map<String, Map<String, Long>> =
        call { rpc.jobs.getStats(GetStatsRequest.newBuilder().setNamespace(namespace).build()) }.queuesList.associate {
            it.queue to
                it.countsMap
        }

    public fun blocking(): BlockingClient = BlockingClient(this)

    override fun close() {
        if (config.channel != null) return
        base.shutdown()
        if (!base.awaitTermination(config.closeTimeout.toMillis(), TimeUnit.MILLISECONDS)) base.shutdownNow()
    }

    private fun job(j: Types.Job?): Job = Job(j ?: error("runnel: answer without a job"), codec)

    private fun inserted(r: Types.Inserted): Inserted = Inserted(r.id, r.duplicate, r.error)
}
