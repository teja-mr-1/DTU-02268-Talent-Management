package dk.dtu.applications.model;

import dk.dtu.applications.config.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

@Service
public class ConformanceChecker {
    private static final Logger log = LoggerFactory.getLogger(ConformanceChecker.class);

    private final String serviceUrl;
    private final Duration timeout;

    @Autowired
    public ConformanceChecker() {
        this(Settings.value("CONFORMANCE_SERVICE_URL", "http://127.0.0.1:8091"), Duration.ofSeconds(30));
    }

    ConformanceChecker(String serviceUrl, Duration timeout) {
        this.serviceUrl = serviceUrl;
        this.timeout = timeout;
    }

    public ConformanceResult check(Path modelFile, Path logFile) {
        if (modelFile == null || !Files.isRegularFile(modelFile)) {
            throw new IllegalArgumentException("DCR model file not found: " + modelFile);
        }
        if (logFile == null || !Files.isRegularFile(logFile)) {
            throw new IllegalArgumentException("XES log file not found: " + logFile);
        }
        try {
            return checkXml(
                    Files.readString(modelFile, StandardCharsets.UTF_8),
                    modelFile.getFileName().toString(),
                    Files.readString(logFile, StandardCharsets.UTF_8),
                    logFile.getFileName().toString());
        } catch (IOException error) {
            throw new RuntimeException("Failed to read DCR model or XES log", error);
        }
    }

    public ConformanceResult checkXml(String modelXml, String modelName, String logXml, String logName) {
        if (modelXml == null || modelXml.isBlank()) throw new IllegalArgumentException("modelXml is required");
        if (logXml == null || logXml.isBlank()) throw new IllegalArgumentException("logXml is required");

        var requests = new JdkClientHttpRequestFactory(
                HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(timeout).build());
        requests.setReadTimeout(timeout);

        ConformanceResult result = RestClient.builder()
                .baseUrl(serviceUrl)
                .requestFactory(requests)
                .build()
                .post()
                .uri("/check")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .body(new CheckRequest(modelXml, logXml, modelName, logName))
                .retrieve()
                .body(ConformanceResult.class);

        if (result == null) throw new IllegalStateException("Empty response from conformance service");
        log.info("Conformance check log={} conformant={} structural={} time={}",
                result.logName(), result.conformant(), result.totalStructuralViolations(), result.totalTimeViolations());
        return result;
    }

    private record CheckRequest(String modelXml, String logXml, String modelName, String logName) {}
}
