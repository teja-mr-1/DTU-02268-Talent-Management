package dk.dtu.applications.api;

import dk.dtu.applications.model.Application;
import dk.dtu.applications.model.EventType;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import dk.dtu.applications.TalentApplication;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import dk.dtu.applications.flink.ApplicationInbox;
import static org.junit.jupiter.api.Assertions.*;

class ApplicationApiTest {
    @Test void validatesQueuesAndReportsBackpressure() throws Exception {
        try (var api = (ServletWebServerApplicationContext) SpringApplication.run(TalentApplication.class,
                "--server.port=0", "--flink.enabled=false", "--camunda.worker.enabled=false", "--intake.capacity=1")) {
            var queue = api.getBean(ApplicationInbox.class);
            queue.startReading(EventType.APPLICATION);
            var client = HttpClient.newHttpClient();
            String address = "http://localhost:" + api.getWebServer().getPort();
            var form = client.send(HttpRequest.newBuilder(URI.create(address + "/")).build(),
                HttpResponse.BodyHandlers.ofString());
            assertEquals(200, form.statusCode());
            assertTrue(form.body().contains("Full name"));
            assertEquals(400, post(client, address, "{invalid").statusCode());
            assertEquals(400, post(client, address, "{}").statusCode());
            assertEquals(400, post(client, address, "{\"name\":\"Ada\",\"position\":\"Other\"}").statusCode());
            assertNull(queue.poll(EventType.APPLICATION, Application.class));

            String body = "{\"name\":\" Ada \",\"position\":\"PhD student\",\"applicationId\":\"untrusted\"}";
            var accepted = post(client, address, body);
            assertEquals(202, accepted.statusCode());
            assertEquals(503, post(client, address, body).statusCode());
            var application = queue.poll(EventType.APPLICATION, Application.class);
            assertEquals("Ada", application.name);
            assertEquals("PhD student", application.position);
            assertNotEquals("untrusted", application.applicationId);
            assertEquals(application.applicationId,
                new ObjectMapper().readTree(accepted.body()).get("applicationId").asText());
            assertTrue(application.receivedAt > 0);
        }
    }

    private HttpResponse<String> post(HttpClient client, String address, String body) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(address + "/api/applications"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }
}
