package dk.dtu.applications.camunda;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import dk.dtu.applications.flink.ApplicationInbox;
import dk.dtu.applications.model.EventType;
import dk.dtu.applications.model.ScaleUpCompletion;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;

class ScaleUpCompletionWorkerTest {
    @Test void forwardsNotificationAndOnlyAcknowledgesAcceptedEvents() throws Exception {
        var completions = new AtomicInteger();
        var activation = new AtomicReference<String>();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v2/jobs/activation", exchange -> {
            activation.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] body = """
                {"jobs":[{"jobKey":"123","processInstanceKey":"456",
                "variables":{"applicationId":"application-1","triggerId":"application-1:scale"}}]}
                """.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.createContext("/v2/jobs/123/completion", exchange -> {
            exchange.getRequestBody().readAllBytes();
            completions.incrementAndGet();
            exchange.sendResponseHeaders(204, -1);
            exchange.close();
        });
        server.start();
        try {
            var inbox = new ApplicationInbox(1);
            var worker = new ScaleUpCompletionWorker(inbox, RestClient.builder()
                .baseUrl("http://127.0.0.1:" + server.getAddress().getPort() + "/v2").build());
            worker.poll(); // Reader not ready: leave the job uncompleted.
            assertEquals(0, completions.get());
            assertNull(inbox.poll(EventType.SCALE_UP_COMPLETED, ScaleUpCompletion.class));
            inbox.startReading(EventType.SCALE_UP_COMPLETED);
            worker.poll();
            assertEquals(1, completions.get());
            worker.poll(); // Inbox full: leave this delivery uncompleted too.
            assertEquals(1, completions.get());
            var event = inbox.poll(EventType.SCALE_UP_COMPLETED, ScaleUpCompletion.class);
            assertNotNull(event);
            assertEquals("456", event.processInstanceKey);
            assertEquals("application-1", event.applicationId);
            assertEquals("application-1:scale", event.triggerId);
            assertTrue(event.receivedAt > 0);
            assertEquals(ScaleUpCompletionWorker.JOB_TYPE,
                new ObjectMapper().readTree(activation.get()).get("type").asText());
        } finally { server.stop(0); }
    }
}
