package dk.dtu.applications.camunda;

import com.sun.net.httpserver.HttpServer;
import dk.dtu.applications.flink.ApplicationInbox;
import dk.dtu.applications.model.EventType;
import dk.dtu.applications.model.HiringTraceEvent;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class HiringTraceWorkerTest {

    @Test
    void pollsCamundaTraceJobsAndForwardsToFlinkInbox() throws Exception {
        AtomicInteger completed = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v2/jobs/activation", exchange -> {
            exchange.getRequestBody().readAllBytes();
            byte[] body = """
                    {"jobs":[{"jobKey":"999","processInstanceKey":"777",
                    "variables":{"vacancyId":"VAC-FP-42"},
                    "customHeaders":{"eventName":"Issue employment contract","role":"HR","terminal":"true"}}]}
                    """.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.createContext("/v2/jobs/999/completion", exchange -> {
            exchange.getRequestBody().readAllBytes();
            completed.incrementAndGet();
            exchange.sendResponseHeaders(204, -1);
            exchange.close();
        });
        server.start();
        try {
            ApplicationInbox inbox = new ApplicationInbox();
            HiringTraceWorker worker = new HiringTraceWorker(
                    inbox,
                    RestClient.builder().baseUrl("http://127.0.0.1:" + server.getAddress().getPort() + "/v2").build());
            worker.poll();

            assertEquals(1, completed.get());
            HiringTraceEvent event = inbox.poll(EventType.HIRING_TRACE_EVENT, HiringTraceEvent.class);
            assertNotNull(event);
            assertEquals("VAC-FP-42", event.vacancyId);
            assertEquals("Issue employment contract", event.eventName);
            assertEquals("HR", event.role);
            assertTrue(event.terminal);
        } finally {
            server.stop(0);
        }
    }
}
