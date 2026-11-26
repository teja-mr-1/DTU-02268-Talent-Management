package dk.dtu.applications.model;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class ConformanceCheckerTest {

    @Test
    void parsesCountsAndComputesConformantFlag() {
        assertEquals(2, ConformanceChecker.parseCount("2 Conforming"));
        assertEquals(0, ConformanceChecker.parseCount("0"));
        assertThrows(IllegalArgumentException.class, () -> ConformanceChecker.parseCount("  "));
        assertThrows(NumberFormatException.class, () -> ConformanceChecker.parseCount("abc"));

        assertTrue(ConformanceResult.of("m.xml", "l.xes", 2, 0, 0, 0, "ok").conformant());
        assertFalse(ConformanceResult.of("m.xml", "l.xes", 1, 0, 1, 2, "bad").conformant());
        assertFalse(ConformanceResult.of("m.xml", "l.xes", 1, 1, 0, 0, "partial").conformant());
    }

    @Test
    void rejectsMissingLogFile(@TempDir Path tempDir) {
        ConformanceChecker checker = new ConformanceChecker("http://127.0.0.1:1/", null, 5000);
        assertThrows(IllegalArgumentException.class, () -> checker.check(tempDir.resolve("missing.xes")));
    }

    @Test
    void checksLogAgainstMockProcessDoctorUi(@TempDir Path tempDir) throws Exception {
        Assumptions.assumeTrue(isBrowserAvailable(), "No Chromium/Chrome binary available for Playwright test");

        byte[] mockHtml = """
                <!doctype html>
                <html><body>
                  <button onclick="document.getElementById('workspace').hidden=false">Conformance</button>
                  <div id="workspace" hidden>
                    <button onclick="document.getElementById('menu').hidden=false">Open Model</button>
                    <div id="menu" hidden>
                      <label>Open Editor XML<input type="file" id="modelFile"></label>
                    </div>
                    <label>Upload Log<input type="file" id="logFile"></label>
                    <button onclick="runCheck()">Check!</button>
                    <div id="counts" hidden>
                      <span title="Conforming">2 Conforming</span>
                      <span title="Partially Violating">0 Partially Violating</span>
                      <span title="Violating">1 Violating</span>
                      <span title="Total Structural Violations">3 Total Structural Violations</span>
                    </div>
                    <ul id="results" hidden><li>Trace Variant #1: Conformant</li></ul>
                  </div>
                  <script>
                    function runCheck() {
                      if (document.getElementById('modelFile').files.length === 1 &&
                          document.getElementById('logFile').files.length === 1) {
                        document.getElementById('counts').hidden = false;
                        document.getElementById('results').hidden = false;
                      }
                    }
                  </script>
                </body></html>
                """.getBytes(StandardCharsets.UTF_8);

        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            exchange.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
            exchange.sendResponseHeaders(200, mockHtml.length);
            exchange.getResponseBody().write(mockHtml);
            exchange.close();
        });
        server.start();
        try {
            ConformanceChecker checker = new ConformanceChecker(
                    "http://127.0.0.1:" + server.getAddress().getPort() + "/", null, 15_000);

            XesLogBuilder logBuilder = new XesLogBuilder();
            logBuilder.appendEvent("VAC-100", "Confirm funding availability", "Head of Finance", Instant.now());
            Path logFile = tempDir.resolve("vac-100.xes");
            logBuilder.publish(logFile);

            ConformanceResult result = checker.check(logFile);
            assertEquals("Full_Professor_Hiring_Compliance.dcr.xml", result.modelName());
            assertEquals("vac-100.xes", result.logName());
            assertEquals(2, result.conforming());
            assertEquals(1, result.violating());
            assertEquals(3, result.totalStructuralViolations());
            assertFalse(result.conformant());
        } finally {
            server.stop(0);
        }
    }

    private static boolean isBrowserAvailable() {
        return ConformanceChecker.hasPlaywrightCache()
                || Files.isExecutable(Path.of("/usr/bin/chromium"))
                || Files.isExecutable(Path.of("/usr/bin/chromium-browser"))
                || Files.isExecutable(Path.of("/usr/bin/google-chrome"));
    }
}
