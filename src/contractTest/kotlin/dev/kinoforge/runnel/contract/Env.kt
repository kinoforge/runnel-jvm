package dev.kinoforge.runnel.contract

import dev.kinoforge.runnel.Backoff
import dev.kinoforge.runnel.ClientConfig
import dev.kinoforge.runnel.RunnelClient
import dev.kinoforge.runnel.Worker
import dev.kinoforge.runnel.WorkerOptions
import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.ClientCall
import io.grpc.ClientInterceptor
import io.grpc.Metadata
import io.grpc.MethodDescriptor
import io.grpc.Status
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import runnel.v1.Admin.CreateKeyRequest
import runnel.v1.Admin.PutNamespaceRequest
import java.security.SecureRandom
import java.time.Duration
import kotlin.time.Duration.Companion.seconds

val URL: String = System.getenv("RUNNEL_URL") ?: "http://127.0.0.1:8700"
val CALLBACK_HOST: String = System.getenv("RUNNEL_CALLBACK_HOST") ?: "127.0.0.1"
val ADMIN_TOKEN: String = System.getenv("RUNNEL_ADMIN_TOKEN") ?: "contract-admin-token-0123456789"
val ALL_ROLES = listOf("producer", "reader", "worker", "admin")
val WAIT: Duration = Duration.ofSeconds(15)

fun fast() = Backoff(base = Duration.ofMillis(100), factor = 1.0, jitter = 0.0)

fun hex(n: Int): String = ByteArray(n).also { SecureRandom().nextBytes(it) }.joinToString("") { "%02x".format(it) }

class Running(
    val worker: Worker,
    private val task: kotlinx.coroutines.Job,
) {
    suspend fun stop() {
        worker.stop()
        task.join()
    }
}

class Env(
    val ns: String,
    val admin: RunnelClient,
    private val scope: CoroutineScope,
) {
    val clients = mutableListOf<RunnelClient>()
    private val workers = mutableListOf<Running>()

    suspend fun token(roles: List<String> = ALL_ROLES): String =
        admin.rpc.admin
            .createKey(
                CreateKeyRequest
                    .newBuilder()
                    .setNamespace(ns)
                    .setName("contract")
                    .addAllRoles(roles)
                    .build(),
            ).token

    suspend fun client(
        roles: List<String> = ALL_ROLES,
        configure: ClientConfig.() -> Unit = {},
    ): RunnelClient {
        val key = token(roles)
        val c =
            RunnelClient(URL, ns) {
                token = key
                configure()
            }
        clients += c
        return c
    }

    fun work(
        c: RunnelClient,
        configure: WorkerOptions.() -> Unit = {},
        handlers: Worker.() -> Unit,
    ): Running {
        val o =
            WorkerOptions().apply {
                lease = Duration.ofSeconds(3)
                shutdownTimeout = Duration.ofSeconds(5)
                claimWait = Duration.ofSeconds(1)
                retryMin = Duration.ofMillis(50)
                configure()
            }
        val w = Worker(c, listOf("q"), o).apply(handlers)
        val r = Running(w, scope.launch { w.run() })
        workers += r
        return r
    }

    suspend fun close() {
        workers.forEach { it.stop() }
        (clients + admin).forEach { it.close() }
    }
}

fun contract(
    callbackSecret: String = "",
    body: suspend CoroutineScope.(Env) -> Unit,
): Unit =
    runBlocking(Dispatchers.Default) {
        withTimeout(90.seconds) {
            val ns = "c" + hex(5)
            val admin = RunnelClient(URL, ns) { token = ADMIN_TOKEN }
            val req = PutNamespaceRequest.newBuilder().setName(ns)
            if (callbackSecret.isNotEmpty()) req.callbackSecret = callbackSecret
            admin.rpc.admin.putNamespace(req.build())
            val e = Env(ns, admin, this)
            try {
                body(e)
            } finally {
                e.close()
            }
        }
    }

suspend fun <T> until(
    timeout: Duration = Duration.ofSeconds(20),
    ok: (T) -> Boolean,
    fn: suspend () -> T,
): T {
    val end = System.nanoTime() + timeout.toNanos()
    while (true) {
        val v = fn()
        if (ok(v) || System.nanoTime() > end) return v
        delay(100)
    }
}

class Dropping(
    private val method: String,
) : ClientInterceptor {
    override fun <Q, R> interceptCall(
        m: MethodDescriptor<Q, R>,
        options: CallOptions,
        next: Channel,
    ): ClientCall<Q, R> {
        if (m.bareMethodName != method) return next.newCall(m, options)
        return object : ClientCall<Q, R>() {
            override fun start(
                listener: Listener<R>,
                headers: Metadata,
            ) = listener.onClose(Status.UNAVAILABLE.withDescription("$method dropped"), Metadata())

            override fun request(numMessages: Int) = Unit

            override fun cancel(
                message: String?,
                cause: Throwable?,
            ) = Unit

            override fun halfClose() = Unit

            override fun sendMessage(message: Q) = Unit
        }
    }
}

inline fun <reified T : Throwable> raises(block: () -> Unit): T {
    try {
        block()
    } catch (e: Throwable) {
        if (e is T) return e
        throw AssertionError("expected ${T::class.simpleName}, got $e", e)
    }
    throw AssertionError("expected ${T::class.simpleName}, nothing was thrown")
}
