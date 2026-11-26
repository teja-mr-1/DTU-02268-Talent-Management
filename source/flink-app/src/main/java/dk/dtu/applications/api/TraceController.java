package dk.dtu.applications.api;

import dk.dtu.applications.flink.ApplicationInbox;
import dk.dtu.applications.flink.VacancyTraceStore;
import dk.dtu.applications.model.EventType;
import dk.dtu.applications.model.HiringTraceEvent;
import dk.dtu.applications.model.IntakeEvent;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
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
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/traces")
public final class TraceController {
    private final ApplicationInbox inbox;
    private final VacancyTraceStore store;

    TraceController(ApplicationInbox inbox, VacancyTraceStore store) {
        this.inbox = inbox;
        this.store = store;
    }

    @PostMapping(value = "/events", consumes = MediaType.APPLICATION_JSON_VALUE)
    ResponseEntity<Map<String, Object>> ingestEvent(@Valid @RequestBody TraceEventRequest request) {
        String vacancyId = request.vacancyId().trim();
        String eventName = request.eventName().trim();
        String role = (request.role() == null || request.role().isBlank()) ? "System" : request.role().trim();
        long timestampMs = parseTimestamp(request.timestamp());

        HiringTraceEvent event = new HiringTraceEvent(vacancyId, eventName, role, timestampMs, request.terminal());
        inbox.offer(new IntakeEvent<>(EventType.HIRING_TRACE_EVENT, event));

        return ResponseEntity.accepted().body(Map.of(
                "vacancyId", vacancyId,
                "eventName", eventName,
                "terminal", request.terminal()));
    }

    @GetMapping
    List<Map<String, Object>> listTraces() {
        return store.snapshotVacancies();
    }

    @GetMapping(value = "/{vacancyId}/xes", produces = MediaType.APPLICATION_XML_VALUE)
    ResponseEntity<String> getXes(@PathVariable("vacancyId") String vacancyId) {
        String xml = store.getXesXml(vacancyId);
        return (xml == null || xml.isBlank())
                ? ResponseEntity.status(HttpStatus.NOT_FOUND).build()
                : ResponseEntity.ok().contentType(MediaType.APPLICATION_XML).body(xml);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    ResponseEntity<Map<String, String>> badRequest(IllegalArgumentException error) {
        return ResponseEntity.badRequest().body(Map.of("error", error.getMessage()));
    }

    private static long parseTimestamp(String raw) {
        if (raw == null || raw.isBlank()) {
            return Instant.now().toEpochMilli();
        }
        try {
            return Instant.parse(raw.trim()).toEpochMilli();
        } catch (DateTimeParseException error) {
            throw new IllegalArgumentException("Invalid ISO-8601 timestamp: " + raw);
        }
    }

    public record TraceEventRequest(
            @NotBlank String vacancyId,
            @NotBlank String eventName,
            String role,
            String timestamp,
            boolean terminal) {
    }
}
