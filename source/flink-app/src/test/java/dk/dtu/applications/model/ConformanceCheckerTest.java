package dk.dtu.applications.model;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class ConformanceCheckerTest {

    @Test
    void computesConformantFlagAcrossViolationTypes() {
        assertTrue(ConformanceResult.of("m.xml", "l.xes", 2, 0, 0, 0, 0, "ok").conformant());
        assertFalse(ConformanceResult.of("m.xml", "l.xes", 1, 0, 1, 2, 0, "bad").conformant());
        assertFalse(ConformanceResult.of("m.xml", "l.xes", 1, 1, 0, 0, 0, "partial").conformant());
        assertFalse(ConformanceResult.of("m.xml", "l.xes", 1, 0, 0, 0, 1, "late").conformant());
    }

    @Test
    void rejectsMissingLogFile(@TempDir Path tempDir) {
        ConformanceChecker checker = new ConformanceChecker("http://127.0.0.1:1", null, Duration.ofSeconds(2));
        assertThrows(IllegalArgumentException.class, () -> checker.check(tempDir.resolve("missing.xes")));
    }

    @Test
    void sendsModelAndLogToConformanceServiceAndParsesVerdict(@TempDir Path tempDir) throws Exception {
        AtomicReference<String> receivedBody = new AtomicReference<>();
        byte[] jsonResponse = """
                {
                  "conforming": 0,
                  "partiallyViolating": 1,
                  "violating": 0,
                  "totalStructuralViolations": 2,
                  "totalTimeViolations": 1,
                  "details": "Trace VAC-100: Partially Violating"
                }
                """.getBytes(StandardCharsets.UTF_8);

        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/check", exchange -> {
            assertEquals("POST", exchange.getRequestMethod());
            receivedBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, jsonResponse.length);
            exchange.getResponseBody().write(jsonResponse);
            exchange.close();
        });
        server.start();
        try {
            ConformanceChecker checker = new ConformanceChecker(
                    "http://127.0.0.1:" + server.getAddress().getPort(), null, Duration.ofSeconds(5));

            XesLogBuilder logBuilder = new XesLogBuilder();
            logBuilder.appendEvent("VAC-100", "Confirm funding availability", "Head of Finance", Instant.now());
            Path logFile = tempDir.resolve("vac-100.xes");
            logBuilder.publish(logFile);

            ConformanceResult result = checker.check(logFile);
            assertEquals("Full_Professor_Hiring_Compliance.dcr.xml", result.modelName());
            assertEquals("vac-100.xes", result.logName());
            assertEquals(0, result.conforming());
            assertEquals(1, result.partiallyViolating());
            assertEquals(0, result.violating());
            assertEquals(2, result.totalStructuralViolations());
            assertEquals(1, result.totalTimeViolations());
            assertFalse(result.conformant());
            assertTrue(receivedBody.get().contains("Ev_01_request"));
            assertTrue(receivedBody.get().contains("Confirm funding availability"));
        } finally {
            server.stop(0);
        }
    }
}
