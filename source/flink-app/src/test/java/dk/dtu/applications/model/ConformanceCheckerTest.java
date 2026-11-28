package dk.dtu.applications.model;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class ConformanceCheckerTest {

    @Test
    void sendsModelAndLogToConformanceServiceAndParsesResult(@TempDir Path tempDir) throws Exception {
        var received = new AtomicReference<String>();
        byte[] response = """
                {"modelName":"rules.dcr.xml","logName":"vac-1.xes","conforming":0,"partiallyViolating":1,
                 "violating":0,"totalStructuralViolations":2,"totalTimeViolations":1,"conformant":false,
                 "details":"Trace VAC-1: Partially Violating"}
                """.getBytes(StandardCharsets.UTF_8);

        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/check", exchange -> {
            assertEquals("POST", exchange.getRequestMethod());
            received.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.start();
        try {
            Path model = tempDir.resolve("rules.dcr.xml");
            Path log = tempDir.resolve("vac-1.xes");
            Files.writeString(model, "<dcr:definitions><dcr:dcrGraph id=\"g\"/></dcr:definitions>");
            Files.writeString(log, "<log><trace><string key=\"concept:name\" value=\"VAC-1\"/></trace></log>");

            var checker = new ConformanceChecker(
                    "http://127.0.0.1:" + server.getAddress().getPort(), Duration.ofSeconds(5));
            ConformanceResult result = checker.check(model, log);

            assertFalse(result.conformant());
            assertEquals("rules.dcr.xml", result.modelName());
            assertEquals("vac-1.xes", result.logName());
            assertEquals(2, result.totalStructuralViolations());
            assertEquals(1, result.totalTimeViolations());
            assertTrue(received.get().contains("dcrGraph"));
            assertTrue(received.get().contains("VAC-1"));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void rejectsMissingFilesAndChecksConformantFlag(@TempDir Path tempDir) throws Exception {
        var checker = new ConformanceChecker("http://127.0.0.1:1", Duration.ofSeconds(2));
        Path model = tempDir.resolve("rules.dcr.xml");
        Files.writeString(model, "<dcr:definitions/>");
        assertThrows(IllegalArgumentException.class,
                () -> checker.check(tempDir.resolve("missing.xml"), tempDir.resolve("a.xes")));
        assertThrows(IllegalArgumentException.class, () -> checker.check(model, tempDir.resolve("missing.xes")));
        assertTrue(new ConformanceResult("m", "l", 1, 0, 0, 0, 0, "ok").conformant());
        assertFalse(new ConformanceResult("m", "l", 0, 0, 1, 1, 0, "bad").conformant());
    }
}
