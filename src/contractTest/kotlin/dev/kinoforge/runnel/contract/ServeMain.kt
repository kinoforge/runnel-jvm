package dev.kinoforge.runnel.contract

import dev.kinoforge.runnel.RunnelClient
import dev.kinoforge.runnel.Worker
import kotlinx.coroutines.delay
import java.time.Duration

fun main() {
    val client = RunnelClient(System.getenv("URL"), System.getenv("NS")) { token = System.getenv("TOKEN") }
    val w = Worker(client, listOf("q")) { shutdownTimeout = Duration.ofSeconds(5) }
    w.handle("ping") { _, _ -> "pong" }
    w.handle("slow") { _, _ ->
        delay(1500)
        "done"
    }
    println("ready")
    w.serve()
}
