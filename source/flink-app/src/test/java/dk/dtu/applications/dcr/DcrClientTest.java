package dk.dtu.applications.dcr;

import com.sun.net.httpserver.HttpServer;
import dk.dtu.applications.model.ScaleUpRequested;
import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import dk.dtu.applications.flink.ApplicationInbox;
import dk.dtu.applications.flink.FlinkRegistry;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DcrClientTest {
    @Test void serializedSinkUpdatesSingletonStateAndSkipsAnActiveProcess() throws Exception {
        var calls = new ArrayList<String>();
        var auth = new ArrayList<String>();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/graphs/2016808", exchange -> {
            String path = exchange.getRequestURI().getPath();
            calls.add(exchange.getRequestMethod() + " " + path);
            auth.add(exchange.getRequestHeaders().getFirst("Authorization"));
            if (exchange.getRequestMethod().equals("GET")) {
                byte[] xml = ("<log><trace id='new' created='2026-11-18T19:17:36.797Z'/>"
                    + "<trace id='old' created='2026-11-18T18:17:36.797Z'/></log>").getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/xml");
                exchange.sendResponseHeaders(200, xml.length);
                exchange.getResponseBody().write(xml);
            } else exchange.sendResponseHeaders(204, -1);
            exchange.close();
        });
        server.start();
        var state = new ScaleUpProcessState();
        String inboxId = FlinkRegistry.register(new ApplicationInbox(), state);
        try {
            var client = new DcrClient("http://127.0.0.1:" + server.getAddress().getPort() + "/graphs/2016808",
                "user", "pass", Duration.ofSeconds(2), Duration.ofSeconds(2));
            var bytes = new ByteArrayOutputStream();
            try (var output = new ObjectOutputStream(bytes)) {
                output.writeObject(new ScaleUpRequestedSink(client, state).forInbox(inboxId));
            }
            ScaleUpRequestedSink sink;
            try (var input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
                sink = (ScaleUpRequestedSink) input.readObject();
            }
            try (var writer = sink.createWriter(null)) {
                writer.write(new ScaleUpRequested(), null);
                assertEquals("new", state.simulationId());
                writer.write(new ScaleUpRequested(), null);
            }
            client.executeEvent(state.simulationId(), "A1");
            client.executeEvent(state.simulationId(), "A12");
            assertEquals(List.of("POST /graphs/2016808/sims", "GET /graphs/2016808/sims/",
                "POST /graphs/2016808/sims/new/events/A1",
                "POST /graphs/2016808/sims/new/events/A12"), calls);
            assertEquals(4, auth.size());
            assertTrue(auth.stream().allMatch("Basic dXNlcjpwYXNz"::equals));
        } finally {
            FlinkRegistry.remove(inboxId);
            server.stop(0);
        }
    }
}
