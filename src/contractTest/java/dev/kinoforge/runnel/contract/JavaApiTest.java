package dev.kinoforge.runnel.contract;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.kinoforge.runnel.BlockingClient;
import dev.kinoforge.runnel.ClientConfig;
import dev.kinoforge.runnel.Job;
import dev.kinoforge.runnel.JobFilter;
import dev.kinoforge.runnel.JobOptions;
import dev.kinoforge.runnel.JobState;
import dev.kinoforge.runnel.Outcomes;
import dev.kinoforge.runnel.RunnelClient;
import dev.kinoforge.runnel.RunnelException;
import dev.kinoforge.runnel.Worker;
import dev.kinoforge.runnel.WorkerOptions;
import io.grpc.Status;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import runnel.v1.Admin;
import runnel.v1.AdminServiceGrpc;

class JavaApiTest {
    private static final Duration WAIT = Duration.ofSeconds(15);

    private static String setUp(String ns) {
        var config = new ClientConfig(EnvKt.getURL(), ns);
        config.setToken(EnvKt.getADMIN_TOKEN());
        try (var admin = new RunnelClient(config)) {
            var stub = AdminServiceGrpc.newBlockingStub(admin.getChannel());
            stub.putNamespace(Admin.PutNamespaceRequest.newBuilder().setName(ns).build());
            var key = Admin.CreateKeyRequest.newBuilder().setNamespace(ns).setName("java").addAllRoles(List.of("producer", "reader", "worker", "admin"));
            return stub.createKey(key.build()).getToken();
        }
    }

    @Test
    void theBlockingClientAndWorkerWorkFromJava() throws Exception {
        var ns = "j" + EnvKt.hex(5);
        var config = new ClientConfig(EnvKt.getURL(), ns);
        config.setToken(setUp(ns));
        try (var client = new BlockingClient(config)) {
            var options = new WorkerOptions();
            options.setLease(Duration.ofSeconds(3));
            options.setShutdownTimeout(Duration.ofSeconds(5));
            var worker = new Worker(client.getClient(), List.of("q"), options);
            worker.handleBlocking("welcome", (job, ctx) -> Map.of("to", job.payload(Map.class).get("to")));
            worker.handleBlocking("strict", (job, ctx) -> {
                throw Outcomes.fail("bad input");
            });
            var running = worker.start();

            var job = client.enqueueAndWait("q", "welcome", Map.of("to", "ana"), new JobOptions(), WAIT);
            assertEquals(JobState.COMPLETED, job.getState());
            assertEquals(Map.of("to", "ana"), job.result(Map.class));

            var strict = new JobOptions();
            strict.setMaxAttempts(3);
            var dead = client.enqueueAndWait("q", "strict", null, strict, WAIT);
            assertEquals(JobState.DEAD, dead.getState());
            assertEquals(1, dead.getAttempt());

            var filter = new JobFilter();
            filter.setState(JobState.DEAD);
            assertEquals(List.of(dead.getId()), client.listJobs(filter).getJobs().stream().map(Job::getId).toList());

            worker.stop();
            running.get(15, TimeUnit.SECONDS);

            assertEquals(JobState.WAITING, client.retry(dead.getId()).getState());
            var other = client.enqueue("other", "noop");
            assertEquals(JobState.CANCELLED, client.cancel(other.getId()).getState());
            var stats = client.stats();
            assertTrue(stats.containsKey("q"), stats.toString());
            assertTrue(stats.containsKey("other"), stats.toString());

            var err = assertThrows(RunnelException.class, () -> client.get("01a1e01b-7fb6-7ace-ab56-982c9c129e34"));
            assertEquals(Status.Code.NOT_FOUND, err.getCode());
        }
    }
}
