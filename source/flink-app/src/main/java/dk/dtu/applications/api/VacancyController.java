package dk.dtu.applications.api;

import dk.dtu.applications.camunda.HiringTraceWorker;
import dk.dtu.applications.config.Settings;
import dk.dtu.applications.flink.ApplicationInbox;
import dk.dtu.applications.flink.VacancyTraceStore;
import dk.dtu.applications.model.ConformanceChecker;
import dk.dtu.applications.model.ConformanceResult;
import dk.dtu.applications.model.EventType;
import dk.dtu.applications.model.HiringTraceEvent;
import dk.dtu.applications.model.IntakeEvent;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Event Source 2 (HR / Department Hiring Milestones), Automation Toggles, Reset, and Live Status API.
 */
@RestController
@RequestMapping("/api/vacancies")
public final class VacancyController {
    private static final Logger log = LoggerFactory.getLogger(VacancyController.class);

    private final ApplicationInbox inbox;
    private final VacancyTraceStore store;
    private final ConformanceChecker checker;
    private final HiringTraceWorker traceWorker;

    VacancyController(
            ApplicationInbox inbox,
            VacancyTraceStore store,
            ConformanceChecker checker,
            HiringTraceWorker traceWorker) {
        this.inbox = inbox;
        this.store = store;
        this.checker = checker;
        this.traceWorker = traceWorker;
    }

    @GetMapping
    ResponseEntity<Map<String, Object>> overview() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("camundaMode", Settings.localCamundaSimulator() ? "local-simulator" : "camunda-8");
        body.put("autoApproveTasks", store.isAutoApproveTasks());
        body.put("autoConformanceCheck", store.isAutoConformanceCheck());
        body.put("totalApplications", store.totalApplications());
        body.put("recentApplications", store.recentApplications());
        body.put("scaleUpAlerts", store.scaleUpAlerts());
        body.put("vacancies", store.snapshotVacancies());
        return ResponseEntity.ok(body);
    }

    @PostMapping("/settings")
    ResponseEntity<Map<String, Object>> updateSettings(@RequestBody SettingsRequest request) {
        if (request.autoApproveTasks() != null) {
            store.setAutoApproveTasks(request.autoApproveTasks());
        }
        if (request.autoConformanceCheck() != null) {
            store.setAutoConformanceCheck(request.autoConformanceCheck());
            if (request.autoConformanceCheck()) {
                for (String pendingId : store.pendingManualUploadVacancyIds()) {
                    triggerAsyncAudit(pendingId);
                }
            }
        }
        log.info("Updated automation toggles autoApproveTasks={} autoConformanceCheck={}",
                store.isAutoApproveTasks(), store.isAutoConformanceCheck());
        return ResponseEntity.ok(Map.of(
                "autoApproveTasks", store.isAutoApproveTasks(),
                "autoConformanceCheck", store.isAutoConformanceCheck()));
    }

    @PostMapping("/{vacancyId}/audit")
    ResponseEntity<Map<String, Object>> auditVacancy(@PathVariable("vacancyId") String vacancyId) {
        String xesPath = store.getXesPath(vacancyId);
        if (xesPath == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "No completed XES log found for " + vacancyId));
        }
        triggerAsyncAudit(vacancyId);
        return ResponseEntity.accepted().body(Map.of("vacancyId", vacancyId, "status", "CHECKING_CONFORMANCE"));
    }

    @PostMapping("/reset")
    ResponseEntity<Map<String, Object>> resetPipeline() {
        String xesDir = Settings.value("XES_OUTPUT_DIR", Path.of("data/xes").toAbsolutePath().toString());
        traceWorker.setResetting(true);
        int cancelledInstances;
        try {
            cancelledInstances = traceWorker.cancelActiveProcessInstances();
            inbox.clearAll();
            store.resetAll(Path.of(xesDir));
            checker.clearHistory();
        } finally {
            traceWorker.setResetting(false);
        }
        log.info("Pipeline reset complete cancelledCamundaInstances={} resetEpoch={}",
                cancelledInstances, store.resetEpoch());
        return ResponseEntity.ok(Map.of(
                "reset", true,
                "cancelledCamundaInstances", cancelledInstances,
                "resetEpoch", store.resetEpoch()));
    }

    private void triggerAsyncAudit(String vacancyId) {
        String xesPath = store.getXesPath(vacancyId);
        String xesXml = store.getXesXml(vacancyId);
        if (xesPath == null) {
            return;
        }
        store.markChecking(vacancyId, List.of(), xesPath, xesXml);
        CompletableFuture.runAsync(() -> {
            try {
                ConformanceResult result = checker.check(Path.of(xesPath));
                store.recordConformanceResult(vacancyId, result);
            } catch (RuntimeException error) {
                store.recordConformanceError(vacancyId, error.getMessage());
            }
        });
    }

    @GetMapping(value = "/{vacancyId}/xes", produces = MediaType.APPLICATION_XML_VALUE)
    ResponseEntity<String> downloadXes(@PathVariable("vacancyId") String vacancyId) {
        String xml = store.getXesXml(vacancyId);
        if (xml == null) {
            return ResponseEntity.notFound().build();
        }
        String safe = vacancyId.replaceAll("[^a-zA-Z0-9._-]", "_");
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + safe + ".xes\"")
                .contentType(MediaType.APPLICATION_XML)
                .body(xml);
    }

    @GetMapping(value = "/dcr-model", produces = MediaType.APPLICATION_XML_VALUE)
    ResponseEntity<String> downloadDcrModel() throws IOException {
        try (InputStream stream = ConformanceChecker.class.getResourceAsStream(ConformanceChecker.DEFAULT_MODEL_RESOURCE)) {
            if (stream == null) {
                return ResponseEntity.notFound().build();
            }
            String xml = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
            return ResponseEntity.ok()
                    .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"Full_Professor_Hiring_Compliance.dcr.xml\"")
                    .contentType(MediaType.APPLICATION_XML)
                    .body(xml);
        }
    }

    @PostMapping("/{vacancyId}/events")
    ResponseEntity<Map<String, Object>> emitEvent(
            @PathVariable("vacancyId") String vacancyId,
            @Valid @RequestBody TraceEventRequest request) {
        long ts = request.timestampEpochMs() != null ? request.timestampEpochMs() : System.currentTimeMillis();
        HiringTraceEvent event = new HiringTraceEvent(
                vacancyId.strip(),
                request.eventName().strip(),
                request.role().strip(),
                ts,
                request.terminal());
        inbox.offer(new IntakeEvent<>(EventType.HIRING_TRACE_EVENT, event));
        log.info("Queued hiring trace event vacancyId={} event='{}' terminal={}",
                event.vacancyId, event.eventName, event.terminal);
        return ResponseEntity.accepted().body(Map.of(
                "vacancyId", event.vacancyId,
                "eventName", event.eventName,
                "role", event.role,
                "terminal", event.terminal));
    }

    @PostMapping("/{vacancyId}/simulate/{scenario}")
    ResponseEntity<Map<String, Object>> simulateScenario(
            @PathVariable("vacancyId") String vacancyId,
            @PathVariable("scenario") String scenario) {
        String id = vacancyId.strip();
        Instant base = Instant.now().minus(30, ChronoUnit.DAYS);

        List<HiringTraceEvent> events = switch (scenario) {
            case "compliant-hire" -> List.of(
                    ev(id, "Request full professor position opening", "Head of Department", base, false),
                    ev(id, "Confirm funding availability", "Head of Finance", base.plus(1, ChronoUnit.DAYS), false),
                    ev(id, "Draft position posting", "Head of Department", base.plus(2, ChronoUnit.DAYS), false),
                    ev(id, "Publish position posting", "HR", base.plus(3, ChronoUnit.DAYS), false),
                    ev(id, "Collect applications", "HR", base.plus(10, ChronoUnit.DAYS), false),
                    ev(id, "Select applicants for assessment", "DTU Appointment Committee", base.plus(11, ChronoUnit.DAYS), false),
                    ev(id, "Notify applicants of their selection", "Head of Department", base.plus(12, ChronoUnit.DAYS), false),
                    ev(id, "Appoint external assessment committee", "Head of Department", base.plus(13, ChronoUnit.DAYS), false),
                    ev(id, "Notify applicants of committee composition", "Head of Department", base.plus(14, ChronoUnit.DAYS), false),
                    ev(id, "Request assessment of candidates", "Head of Department", base.plus(15, ChronoUnit.DAYS), false),
                    ev(id, "Assess selected candidates", "External Assessment Committee", base.plus(18, ChronoUnit.DAYS), false),
                    ev(id, "Inform DTU of selected candidates", "External Assessment Committee", base.plus(19, ChronoUnit.DAYS), false),
                    ev(id, "Candidate list received", "Head of Department", base.plus(20, ChronoUnit.DAYS), false),
                    ev(id, "Review selected candidates list (Dean)", "Dean", base.plus(21, ChronoUnit.DAYS), false),
                    ev(id, "Review selected candidates list (Hiring Group)", "DTU Compute Hiring Group", base.plus(21, ChronoUnit.DAYS), false),
                    ev(id, "Select candidates for evaluation", "DTU Compute Hiring Group", base.plus(22, ChronoUnit.DAYS), false),
                    ev(id, "Visit candidates' lecture and evaluate teaching performance", "DTU Compute Hiring Group", base.plus(23, ChronoUnit.DAYS), false),
                    ev(id, "Invite candidates to interviews", "DTU Compute Hiring Group", base.plus(24, ChronoUnit.DAYS), false),
                    ev(id, "Conduct first round interviews", "DTU Compute Hiring Group", base.plus(25, ChronoUnit.DAYS), false),
                    ev(id, "Conduct second round interviews", "DTU Compute Hiring Group", base.plus(26, ChronoUnit.DAYS), false),
                    ev(id, "Determine most qualified candidate", "DTU Compute Hiring Group", base.plus(27, ChronoUnit.DAYS), false),
                    ev(id, "Send offer to preferred candidate", "HR", base.plus(28, ChronoUnit.DAYS), false),
                    ev(id, "Wait for candidate response", "HR", base.plus(29, ChronoUnit.DAYS), false),
                    ev(id, "Issue employment contract", "HR", base.plus(30, ChronoUnit.DAYS), true)
            );
            case "compliant-close-unfilled" -> List.of(
                    ev(id, "Request full professor position opening", "Head of Department", base, false),
                    ev(id, "Confirm funding availability", "Head of Finance", base.plus(1, ChronoUnit.DAYS), false),
                    ev(id, "Draft position posting", "Head of Department", base.plus(2, ChronoUnit.DAYS), false),
                    ev(id, "Publish position posting", "HR", base.plus(3, ChronoUnit.DAYS), false),
                    ev(id, "Close position unfilled", "HR", base.plus(25, ChronoUnit.DAYS), true)
            );
            case "violation-skip-committee" -> List.of(
                    ev(id, "Request full professor position opening", "Head of Department", base, false),
                    ev(id, "Confirm funding availability", "Head of Finance", base.plus(1, ChronoUnit.DAYS), false),
                    ev(id, "Draft position posting", "Head of Department", base.plus(2, ChronoUnit.DAYS), false),
                    ev(id, "Publish position posting", "HR", base.plus(3, ChronoUnit.DAYS), false),
                    ev(id, "Collect applications", "HR", base.plus(10, ChronoUnit.DAYS), false),
                    ev(id, "Conduct first round interviews", "DTU Compute Hiring Group", base.plus(20, ChronoUnit.DAYS), false),
                    ev(id, "Issue employment contract", "HR", base.plus(25, ChronoUnit.DAYS), true)
            );
            case "violation-never-closed" -> List.of(
                    ev(id, "Request full professor position opening", "Head of Department", base, false),
                    ev(id, "Confirm funding availability", "Head of Finance", base.plus(1, ChronoUnit.DAYS), false),
                    ev(id, "Draft position posting", "Head of Department", base.plus(2, ChronoUnit.DAYS), false),
                    ev(id, "Publish position posting", "HR", base.plus(3, ChronoUnit.DAYS), false),
                    ev(id, "Collect applications", "HR", base.plus(10, ChronoUnit.DAYS), true)
            );
            default -> null;
        };

        if (events == null) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "Unknown scenario. Use: compliant-hire, compliant-close-unfilled, violation-skip-committee, violation-never-closed"));
        }

        for (HiringTraceEvent event : events) {
            inbox.offer(new IntakeEvent<>(EventType.HIRING_TRACE_EVENT, event));
        }
        return ResponseEntity.accepted().body(Map.of(
                "vacancyId", id,
                "scenario", scenario,
                "eventsQueued", events.size()));
    }

    private static HiringTraceEvent ev(String vacancyId, String name, String role, Instant ts, boolean terminal) {
        return new HiringTraceEvent(vacancyId, name, role, ts.toEpochMilli(), terminal);
    }

    public record TraceEventRequest(
            @NotBlank String eventName,
            @NotBlank String role,
            boolean terminal,
            Long timestampEpochMs) {
    }

    public record SettingsRequest(
            Boolean autoApproveTasks,
            Boolean autoConformanceCheck) {
    }
}
