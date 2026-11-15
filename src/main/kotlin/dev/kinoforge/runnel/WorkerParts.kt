package dev.kinoforge.runnel

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import runnel.v1.Types
import java.net.InetAddress
import java.security.SecureRandom
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CancellationException

public typealias Handler = suspend (job: Job, ctx: JobContext) -> Any?

public fun interface BlockingHandler {
    public fun handle(
        job: Job,
        ctx: JobContext,
    ): Any?
}

public enum class WorkMode { STREAM, POLL }

public class WorkerOptions {
    public var id: String = defaultWorkerId()
    public var concurrency: Int = 10
    public var lease: Duration = Duration.ofSeconds(30)
    public var shutdownTimeout: Duration = Duration.ofSeconds(30)
    public var mode: WorkMode = WorkMode.STREAM
    public var claimWait: Duration = Duration.ofSeconds(20)
    public var finishTries: Int = 6
    public var retryMin: Duration = Duration.ofMillis(200)
    public var retryMax: Duration = Duration.ofSeconds(5)
    public var blockingDispatcher: CoroutineDispatcher = Dispatchers.IO
    public var logger: Logger = LoggerFactory.getLogger("runnel")
}

public fun defaultWorkerId(): String {
    val host = runCatching { InetAddress.getLocalHost().hostName }.getOrDefault("").replace(Regex("[^A-Za-z0-9._-]"), "-").take(64)
    val suffix = ByteArray(3).also { SecureRandom().nextBytes(it) }.joinToString("") { "%02x".format(it) }
    return "${host.ifEmpty { "worker" }}-${ProcessHandle.current().pid()}-$suffix"
}

internal class Handlers(
    val run: Handler,
    val poll: Handler?,
)

internal class Run(
    val pb: Types.Job,
    val job: Job,
    val lease: Duration,
    @Volatile var leaseUntil: Instant,
) {
    @Volatile var reason: CancellationException? = null
        private set

    @Volatile var lost: Boolean = false
        private set

    @Volatile var done: kotlinx.coroutines.Job? = null
    private var task: kotlinx.coroutines.Job? = null

    fun attach(t: kotlinx.coroutines.Job) {
        val r =
            synchronized(this) {
                task = t
                reason
            }
        if (r != null) t.cancel(r)
    }

    fun cancel(r: CancellationException) {
        val t =
            synchronized(this) {
                if (reason != null) return
                reason = r
                task
            }
        t?.cancel(r)
    }

    fun markLost(): Boolean =
        synchronized(this) {
            if (lost) return false
            lost = true
            true
        }
}

public class JobContext internal constructor(
    public val worker: Worker,
    private val run: Run,
) {
    public val isCancelled: Boolean get() = run.reason != null
    public val reason: CancellationException? get() = run.reason

    public suspend fun dependencies(): List<Job> = worker.dependencies(run.job)

    public fun dependenciesBlocking(): List<Job> = runBlocking { dependencies() }
}
