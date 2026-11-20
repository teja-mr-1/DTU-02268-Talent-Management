package dk.dtu.applications.api;

import dk.dtu.applications.model.ConformanceChecker;
import dk.dtu.applications.model.ConformanceResult;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/conformance")
public final class ConformanceController {
    private final ConformanceChecker checker;

    ConformanceController(ConformanceChecker checker) {
        this.checker = checker;
    }

    @GetMapping
    ResponseEntity<Map<String, Object>> summary() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("model", checker.configuredModelName());
        body.put("latestResult", checker.latestResult().orElse(null));
        body.put("history", checker.recentResults());
        return ResponseEntity.ok(body);
    }

    @PostMapping(value = "/check", consumes = MediaType.APPLICATION_JSON_VALUE)
    ResponseEntity<ConformanceResult> checkJson(@Valid @RequestBody CheckRequest request) {
        ConformanceResult result = checker.checkXml(request.xesXml(), request.modelXml(), request.logName());
        return ResponseEntity.ok(result);
    }

    @PostMapping(value = "/check", consumes = {MediaType.APPLICATION_XML_VALUE, MediaType.TEXT_XML_VALUE, MediaType.TEXT_PLAIN_VALUE})
    ResponseEntity<ConformanceResult> checkRawXml(@RequestBody String xesXml) {
        ConformanceResult result = checker.checkXml(xesXml, null, "raw-upload.xes");
        return ResponseEntity.ok(result);
    }

    @PostMapping(value = "/check", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    ResponseEntity<ConformanceResult> checkMultipart(
            @RequestParam("file") MultipartFile logFile,
            @RequestParam(value = "model", required = false) MultipartFile modelFile) throws IOException {
        if (logFile.isEmpty()) {
            throw new IllegalArgumentException("Uploaded XES file must not be empty");
        }
        String xesXml = new String(logFile.getBytes(), StandardCharsets.UTF_8);
        String modelXml = (modelFile != null && !modelFile.isEmpty())
                ? new String(modelFile.getBytes(), StandardCharsets.UTF_8)
                : null;
        String logName = logFile.getOriginalFilename() != null ? logFile.getOriginalFilename() : "uploaded.xes";
        ConformanceResult result = checker.checkXml(xesXml, modelXml, logName);
        return ResponseEntity.ok(result);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    ResponseEntity<Map<String, String>> badRequest(IllegalArgumentException error) {
        return ResponseEntity.badRequest().body(Map.of("error", error.getMessage()));
    }

    @ExceptionHandler(ConformanceChecker.ConformanceCheckException.class)
    ResponseEntity<Map<String, String>> checkerFailed(ConformanceChecker.ConformanceCheckException error) {
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body(Map.of("error", error.getMessage()));
    }

    public record CheckRequest(
            @NotBlank String xesXml,
            String modelXml,
            String logName) {
    }
}
