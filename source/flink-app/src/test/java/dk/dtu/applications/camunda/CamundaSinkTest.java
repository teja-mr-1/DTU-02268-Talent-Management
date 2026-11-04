package dk.dtu.applications.camunda;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import dk.dtu.applications.config.CamundaClientFactory;
import dk.dtu.applications.model.Application;
import dk.dtu.applications.model.ApplicationReceived;
import dk.dtu.applications.model.ScaleUpRequested;
import org.junit.jupiter.api.Test;
import java.io.*;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class CamundaSinkTest {
    @Test void serializedSinkUsesSharedAuthenticationAndSendsEachCommandOnce() throws Exception {
        List<JsonNode> requests = new ArrayList<>();
        List<String> authorization = new ArrayList<>();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v2/process-instances", exchange -> {
            authorization.add(exchange.getRequestHeaders().getFirst("Authorization"));
            requests.add(new ObjectMapper().readTree(exchange.getRequestBody()));
            exchange.sendResponseHeaders(requests.size() == 3 ? 503 : 200, -1);
            exchange.close();
        });
        server.start();
        try {
            var factory = new CamundaClientFactory("http://127.0.0.1:" + server.getAddress().getPort() + "/v2",
                "demo", "password", "test-token", Duration.ofSeconds(5), Duration.ofSeconds(15));
            var bytes = new ByteArrayOutputStream();
            try (var output = new ObjectOutputStream(bytes)) { output.writeObject(new ApplicationReceivedSink(factory)); output.writeObject(new ScaleUpRequestedSink(factory)); }
            ApplicationReceivedSink restored;
            ScaleUpRequestedSink scaleUp;
            try (var input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
                restored = (ApplicationReceivedSink) input.readObject();
                scaleUp = (ScaleUpRequestedSink) input.readObject();
            }
            var application = new Application();
            application.applicationId = "application-1";
            application.name = "Ada";
            application.position = "PhD student";
            try (var writer = restored.createWriter(null); var scaleWriter = scaleUp.createWriter(null)) {
                writer.write(new ApplicationReceived(application, 6), null);
                scaleWriter.write(new ScaleUpRequested(application, 6), null);
                // Preserve existing behavior: log rejected responses, without retrying.
                writer.write(new ApplicationReceived(application, 6), null);
            }
            assertEquals(List.of("Bearer test-token", "Bearer test-token", "Bearer test-token"), authorization);
            assertEquals(3, requests.size());
            assertEquals("application-received", requests.get(0).get("processDefinitionId").asText());
            assertEquals("scale-up-application-receival", requests.get(1).get("processDefinitionId").asText());
            assertEquals("application-1:scale", requests.get(1).path("variables").path("triggerId").asText());
            assertEquals(6, requests.get(1).path("variables").path("applicationsInLastHour").asInt());
        } finally { server.stop(0); }
    }
}
