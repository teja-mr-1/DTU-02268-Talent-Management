package dk.dtu.applications.api;

import com.sun.net.httpserver.HttpServer;
import dk.dtu.applications.dcr.DcrClient;
import dk.dtu.applications.dcr.ScaleUpProcessState;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;
import static org.junit.jupiter.api.Assertions.*;

class ManagerApplicationControllerTest {
    @Test void managerActionsExecuteCorrespondingEventsAndReportFailures() throws Exception {
        var calls = new ArrayList<String>();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            calls.add(exchange.getRequestMethod() + " " + exchange.getRequestURI().getPath());
            exchange.sendResponseHeaders(calls.size() == 3 ? 503 : 204, -1);
            exchange.close();
        });
        server.start();
        var state = new ScaleUpProcessState();
        try {
            var client = new DcrClient("http://127.0.0.1:" + server.getAddress().getPort(),
                "user", "pass", Duration.ofSeconds(2), Duration.ofSeconds(2));
            var controller = new ManagerApplicationController(client, state);
            assertEquals(409, assertThrows(ResponseStatusException.class,
                controller::getPendingApplications).getStatusCode().value());
            assertTrue(calls.isEmpty());
            state.setSimulationId("2109385");
            assertEquals(List.of(), controller.getPendingApplications());
            assertEquals(204, controller.distributeApplications().getStatusCode().value());
            assertEquals(502, assertThrows(ResponseStatusException.class,
                controller::distributeApplications).getStatusCode().value());
            assertEquals(List.of("POST /sims/2109385/events/A1", "POST /sims/2109385/events/A12",
                "POST /sims/2109385/events/A12"), calls);
        } finally {
            server.stop(0);
        }
    }
}
