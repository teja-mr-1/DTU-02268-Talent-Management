package dk.dtu.applications.api;

import dk.dtu.applications.flink.ApplicationInbox;
import dk.dtu.applications.flink.VacancyTraceStore;
import dk.dtu.applications.model.ConformanceChecker;
import dk.dtu.applications.model.ConformanceResult;
import dk.dtu.applications.model.EventType;
import dk.dtu.applications.model.HiringTraceEvent;
import dk.dtu.applications.model.IntakeEvent;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Step 3 REST API for receiving workflow trace events (e.g. from Camunda 8 Outbound
 * REST Connectors), inspecting per-vacancy traces/XES logs, and triggering Process
 * Doctor DCR conformance checks.
 */
@RestController
@RequestMapping("/api/traces")
public final class TraceController {
    private final ApplicationInbox inbox;
    private final VacancyTraceStore traceStore;
    private final ConformanceChecker conformanceChecker;

    TraceController(
            ApplicationInbox inbox,
            VacancyTraceStore traceStore,
            ConformanceChecker conformanceChecker) {
        this.inbox = inbox;
        this.traceStore = traceStore;
        this.conformanceChecker = conformanceChecker;
    }

    @PostMapping(value = "/events", consumes = MediaType.APPLICATION_JSON_VALUE)
    ResponseEntity<Map<String, Object>> ingestEvent(@Valid @RequestBody TraceEventRequest request) {
        String vacancyId = request.vacancyId().trim();
        String eventName = request.eventName().trim();
        String role = normalizeRole(request.role());
        long timestampMs = parseTimestamp(request.timestamp());
        boolean terminal = request.terminal();

        HiringTraceEvent event = new HiringTraceEvent(vacancyId, eventName, role, timestampMs, terminal);
        inbox.offer(new IntakeEvent<>(EventType.HIRING_TRACE_EVENT, event));

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", "accepted");
        body.put("vacancyId", vacancyId);
        body.put("eventName", eventName);
        body.put("role", role);
        body.put("timestamp", Instant.ofEpochMilli(timestampMs).toString());
        body.put("terminal", terminal);
        return ResponseEntity.accepted().body(body);
    }

    @PostMapping(value = "/batch", consumes = MediaType.APPLICATION_JSON_VALUE)
    ResponseEntity<Map<String, Object>> ingestBatch(@Valid @RequestBody TraceBatchRequest request) {
        String vacancyId = request.vacancyId().trim();
        boolean finalizeTrace = request.terminal() == null || request.terminal();
        List<BatchEventItem> items = request.events();
        long baseMs = Instant.now().toEpochMilli();

        for (int i = 0; i < items.size(); i++) {
            BatchEventItem item = items.get(i);
            String eventName = item.eventName().trim();
            String role = normalizeRole(item.role());
            long ts = (item.timestamp() == null || item.timestamp().isBlank())
                    ? baseMs + i
                    : parseTimestamp(item.timestamp());
            boolean isTerminal = finalizeTrace && (i == items.size() - 1);
            HiringTraceEvent event = new HiringTraceEvent(vacancyId, eventName, role, ts, isTerminal);
            inbox.offer(new IntakeEvent<>(EventType.HIRING_TRACE_EVENT, event));
        }

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", "accepted");
        body.put("vacancyId", vacancyId);
        body.put("acceptedEvents", items.size());
        body.put("terminal", finalizeTrace);
        return ResponseEntity.accepted().body(body);
    }

    @GetMapping
    ResponseEntity<Map<String, Object>> listTraces() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("model", conformanceChecker.configuredModelName());
        body.put("autoConformanceCheck", traceStore.isAutoConformanceCheck());
        body.put("vacancies", traceStore.snapshotVacancies());
        return ResponseEntity.ok(body);
    }

    @GetMapping("/{vacancyId}")
    ResponseEntity<Map<String, Object>> getTrace(@PathVariable("vacancyId") String vacancyId) {
        return traceStore.snapshotVacancy(vacancyId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(Map.of("error", "No trace found for vacancyId: " + vacancyId)));
    }

    @GetMapping(value = "/{vacancyId}/xes", produces = MediaType.APPLICATION_XML_VALUE)
    ResponseEntity<String> downloadXes(@PathVariable("vacancyId") String vacancyId) {
        String xml = traceStore.getXesXml(vacancyId);
        if (xml == null || xml.isBlank()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
        }
        String safeFile = vacancyId.replaceAll("[^a-zA-Z0-9._-]", "_") + ".xes";
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + safeFile + "\"")
                .contentType(MediaType.APPLICATION_XML)
                .body(xml);
    }

    @PostMapping("/{vacancyId}/check")
    ResponseEntity<?> checkVacancy(
            @PathVariable("vacancyId") String vacancyId,
            @RequestBody(required = false) ManualCheckRequest request) {
        String xml = traceStore.getXesXml(vacancyId);
        if (xml == null || xml.isBlank()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", "No published XES log available for vacancyId: " + vacancyId));
        }
        String modelXml = request != null ? request.modelXml() : null;
        String safeLogName = vacancyId.replaceAll("[^a-zA-Z0-9._-]", "_") + ".xes";
        try {
            ConformanceResult result = conformanceChecker.checkXml(xml, modelXml, safeLogName);
            traceStore.recordConformanceResult(vacancyId, result);
            return ResponseEntity.ok(result);
        } catch (ConformanceChecker.ConformanceCheckException error) {
            traceStore.recordConformanceError(vacancyId, error.getMessage());
            throw error;
        }
    }

    @ExceptionHandler(IllegalArgumentException.class)
    ResponseEntity<Map<String, String>> badRequest(IllegalArgumentException error) {
        return ResponseEntity.badRequest().body(Map.of("error", error.getMessage()));
    }

    @ExceptionHandler(ConformanceChecker.ConformanceCheckException.class)
    ResponseEntity<Map<String, String>> checkerFailed(ConformanceChecker.ConformanceCheckException error) {
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body(Map.of("error", error.getMessage()));
    }

    private static String normalizeRole(String role) {
        return (role == null || role.isBlank()) ? "System" : role.trim();
    }

    private static long parseTimestamp(String raw) {
        if (raw == null || raw.isBlank()) {
            return Instant.now().toEpochMilli();
        }
        String trimmed = raw.trim();
        try {
            return Instant.parse(trimmed).toEpochMilli();
        } catch (DateTimeParseException ignored) {
            try {
                return Long.parseLong(trimmed);
            } catch (NumberFormatException error) {
                throw new IllegalArgumentException(
                        "Invalid timestamp '" + raw + "'. Use ISO-8601 (e.g. 2026-11-26T12:00:00Z) or epoch milliseconds.");
            }
        }
    }

    public record TraceEventRequest(
            @NotBlank String vacancyId,
            @NotBlank String eventName,
            String role,
            String timestamp,
            boolean terminal) {
    }

    public record TraceBatchRequest(
            @NotBlank String vacancyId,
            @NotEmpty List<@Valid BatchEventItem> events,
            Boolean terminal) {
    }

    public record BatchEventItem(
            @NotBlank String eventName,
            String role,
            String timestamp) {
    }

    public record ManualCheckRequest(String modelXml) {
    }
}
