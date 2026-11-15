package dev.kinoforge.runnel

import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.ClientCall
import io.grpc.ClientInterceptor
import io.grpc.ForwardingClientCall
import io.grpc.Grpc
import io.grpc.InsecureChannelCredentials
import io.grpc.ManagedChannel
import io.grpc.Metadata
import io.grpc.MethodDescriptor
import io.grpc.TlsChannelCredentials
import java.net.URI
import java.net.URISyntaxException

internal data class Target(
    val host: String,
    val port: Int,
    val tls: Boolean,
)

internal fun target(url: String): Target {
    val u =
        try {
            URI(url)
        } catch (e: URISyntaxException) {
            throw IllegalArgumentException("runnel: bad url \"$url\"", e)
        }
    val tls =
        when (u.scheme) {
            "http" -> false
            "https" -> true
            else -> throw IllegalArgumentException("runnel: url must start with http:// or https://: \"$url\"")
        }
    val host =
        u.host?.removeSurrounding("[", "]")?.takeIf { it.isNotEmpty() }
            ?: throw IllegalArgumentException("runnel: url has no host: \"$url\"")
    val port = if (u.port == -1) (if (tls) 443 else 80) else u.port
    return Target(host, port, tls)
}

private val AUTHORIZATION: Metadata.Key<String> = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER)

internal class Auth(
    private val token: String,
) : ClientInterceptor {
    override fun <Q, R> interceptCall(
        method: MethodDescriptor<Q, R>,
        options: CallOptions,
        next: Channel,
    ): ClientCall<Q, R> =
        object : ForwardingClientCall.SimpleForwardingClientCall<Q, R>(next.newCall(method, options)) {
            override fun start(
                listener: Listener<R>,
                headers: Metadata,
            ) {
                headers.removeAll(AUTHORIZATION)
                headers.put(AUTHORIZATION, "Bearer $token")
                super.start(listener, headers)
            }
        }
}

internal fun buildChannel(config: ClientConfig): ManagedChannel {
    val t = target(config.url)
    val creds = if (t.tls) config.credentials ?: TlsChannelCredentials.create() else InsecureChannelCredentials.create()
    val builder = Grpc.newChannelBuilderForAddress(t.host, t.port, creds)
    config.channelBuilder?.accept(builder)
    return builder.build()
}
