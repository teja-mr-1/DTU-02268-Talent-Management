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
import java.io.InputStream;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

/**
 * Checks generated XES hiring logs against a DCR compliance model by calling
 * the lightweight dcr-engine conformance service over HTTP.
 */
@Service
public class ConformanceChecker {
    private static final Logger log = LoggerFactory.getLogger(ConformanceChecker.class);
    public static final String DEFAULT_SERVICE_URL = "http://127.0.0.1:8091";
    public static final String DEFAULT_MODEL_RESOURCE = "/dcr/Full_Professor_Hiring_Compliance.dcr.xml";

    private final String serviceUrl;
    private final Path configuredModelPath;
    private final Duration timeout;

    @Autowired
    public ConformanceChecker() {
        String modelPath = Settings.value("DCR_MODEL_PATH", "").trim();
        this.serviceUrl = Settings.value("CONFORMANCE_SERVICE_URL", DEFAULT_SERVICE_URL);
        this.configuredModelPath = modelPath.isEmpty() ? null : Path.of(modelPath);
        this.timeout = Duration.ofMillis(Long.parseLong(Settings.value("CONFORMANCE_TIMEOUT_MS", "30000")));
    }

    ConformanceChecker(String serviceUrl, Path configuredModelPath, Duration timeout) {
        this.serviceUrl = serviceUrl;
        this.configuredModelPath = configuredModelPath;
        this.timeout = timeout;
    }

    public ConformanceResult check(Path logFile) {
        try {
            String modelName;
            String modelXml;
            if (configuredModelPath != null) {
                if (!Files.isRegularFile(configuredModelPath)) {
                    throw new IllegalArgumentException("DCR model file not found: " + configuredModelPath);
                }
                modelName = configuredModelPath.getFileName().toString();
                modelXml = Files.readString(configuredModelPath, StandardCharsets.UTF_8);
            } else {
                modelName = Path.of(DEFAULT_MODEL_RESOURCE).getFileName().toString();
                modelXml = loadDefaultModelXml();
            }
            return runCheck(modelXml, modelName, logFile);
        } catch (IOException error) {
            throw new RuntimeException("Failed to read DCR model or XES log", error);
        }
    }

    public ConformanceResult check(Path modelFile, Path logFile) {
        if (modelFile == null || !Files.isRegularFile(modelFile)) {
            throw new IllegalArgumentException("DCR model file not found: " + modelFile);
        }
        try {
            String modelXml = Files.readString(modelFile, StandardCharsets.UTF_8);
            return runCheck(modelXml, modelFile.getFileName().toString(), logFile);
        } catch (IOException error) {
            throw new RuntimeException("Failed to read DCR model or XES log", error);
        }
    }

    private ConformanceResult runCheck(String modelXml, String modelName, Path logFile) throws IOException {
        if (logFile == null || !Files.isRegularFile(logFile)) {
            throw new IllegalArgumentException("XES log file not found: " + logFile);
        }
        String logName = logFile.getFileName().toString();
        String logXml = Files.readString(logFile, StandardCharsets.UTF_8);

        log.info("Running DCR conformance check via dcr-engine service={} model={} log={}",
                serviceUrl, modelName, logName);

        var http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(timeout)
                .build();
        var requestFactory = new JdkClientHttpRequestFactory(http);
        requestFactory.setReadTimeout(timeout);

        CheckResponse response = RestClient.builder()
                .baseUrl(serviceUrl)
                .requestFactory(requestFactory)
                .build()
                .post()
                .uri("/check")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .body(new CheckRequest(modelXml, logXml, modelName, logName))
                .retrieve()
                .body(CheckResponse.class);

        if (response == null) {
            throw new IllegalStateException("Empty response from conformance service");
        }

        ConformanceResult result = ConformanceResult.of(
                modelName,
                logName,
                response.conforming(),
                response.partiallyViolating(),
                response.violating(),
                response.totalStructuralViolations(),
                response.totalTimeViolations(),
                response.details() != null ? response.details() : "");

        log.info("Conformance result log={} conformant={} conforming={} partial={} violating={} structural={} time={}",
                logName, result.conformant(), result.conforming(), result.partiallyViolating(),
                result.violating(), result.totalStructuralViolations(), result.totalTimeViolations());
        return result;
    }

    private static String loadDefaultModelXml() throws IOException {
        try (InputStream stream = ConformanceChecker.class.getResourceAsStream(DEFAULT_MODEL_RESOURCE)) {
            if (stream == null) {
                throw new IOException("Bundled DCR model not found: " + DEFAULT_MODEL_RESOURCE);
            }
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private record CheckRequest(String modelXml, String logXml, String modelName, String logName) {}

    private record CheckResponse(
            int conforming,
            int partiallyViolating,
            int violating,
            int totalStructuralViolations,
            int totalTimeViolations,
            String details) {}
}
