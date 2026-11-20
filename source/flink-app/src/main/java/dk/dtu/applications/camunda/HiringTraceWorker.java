package dk.dtu.applications.camunda;

import com.fasterxml.jackson.databind.JsonNode;
import dk.dtu.applications.config.Settings;
import dk.dtu.applications.flink.ApplicationInbox;
import dk.dtu.applications.flink.VacancyTraceStore;
import dk.dtu.applications.model.EventType;
import dk.dtu.applications.model.HiringTraceEvent;
import dk.dtu.applications.model.IntakeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.util.List;
import java.util.Map;

/**
 * Step 2 Camunda 8 job worker: polls Zeebe for "emit-hiring-trace" tasks/listeners
 * across all 6 lanes + External Assessment Committee pool of the full Hiring Full Professor BPMN,
 * optionally auto-completes pending Camunda User Tasks when Auto-Approve is enabled,
 * and forwards each milestone into Flink's per-vacancy trace stream.
 */
@Component
public final class HiringTraceWorker {
    static final String JOB_TYPE = "emit-hiring-trace";
    private static final Logger log = LoggerFactory.getLogger(HiringTraceWorker.class);

    public record ElementMeta(String eventName, String role, boolean terminal) {
    }

    private static final Map<String, ElementMeta> FULL_BPMN_ELEMENTS = Map.ofEntries(
            Map.entry("Activity_1pvybmx", new ElementMeta("Request full professor position opening", "Head of Department", false)),
            Map.entry("Activity_0v8euig", new ElementMeta("Confirm funding availability", "Head of Finance", false)),
            Map.entry("Event_0jsqyzw", new ElementMeta("Close position unfilled", "HR", true)),
            Map.entry("Activity_0x01z2c", new ElementMeta("Draft position posting", "Head of Department", false)),
            Map.entry("Activity_0argylm", new ElementMeta("Publish position posting", "HR", false)),
            Map.entry("Activity_0mgz0pr", new ElementMeta("Collect applications", "HR", false)),
            Map.entry("Activity_02yrc3f", new ElementMeta("Select applicants for assessment", "DTU Appointment Committee", false)),
            Map.entry("Event_0o2x47v", new ElementMeta("Notify applicants of their selection", "Head of Department", false)),
            Map.entry("Activity_0sbu73t", new ElementMeta("Appoint external assessment committee", "Head of Department", false)),
            Map.entry("Event_1uzndin", new ElementMeta("Notify applicants of committee composition", "Head of Department", false)),
            Map.entry("Activity_1ynuleo", new ElementMeta("Request assessment of candidates", "Head of Department", false)),
            Map.entry("Activity_1m6czrg", new ElementMeta("Assess selected candidates", "External Assessment Committee", false)),
            Map.entry("Activity_0aiaz01", new ElementMeta("Inform DTU of selected candidates", "External Assessment Committee", false)),
            Map.entry("Event_19jwxxv", new ElementMeta("Candidate list received", "Head of Department", false)),
            Map.entry("Activity_08grn1y", new ElementMeta("Review selected candidates list (Dean)", "Dean", false)),
            Map.entry("Activity_0g0iy0f", new ElementMeta("Review selected candidates list (Hiring Group)", "DTU Compute Hiring Group", false)),
            Map.entry("Activity_0ecyg60", new ElementMeta("Select candidates for evaluation", "DTU Compute Hiring Group", false)),
            Map.entry("Activity_02r7m32", new ElementMeta("Visit candidates' lecture and evaluate teaching performance", "DTU Compute Hiring Group", false)),
            Map.entry("Activity_0iyaxc5", new ElementMeta("Invite candidates to interviews", "DTU Compute Hiring Group", false)),
            Map.entry("Activity_14y5o5q", new ElementMeta("Conduct first round interviews", "DTU Compute Hiring Group", false)),
            Map.entry("Activity_0oqtlty", new ElementMeta("Conduct second round interviews", "DTU Compute Hiring Group", false)),
            Map.entry("Activity_19fhkff", new ElementMeta("Determine most qualified candidate", "DTU Compute Hiring Group", false)),
            Map.entry("Activity_15emhmi", new ElementMeta("Send offer to preferred candidate", "HR", false)),
            Map.entry("Activity_1us9vw0", new ElementMeta("Wait for candidate response", "HR", false)),
            Map.entry("Activity_1yx7601", new ElementMeta("Issue employment contract", "HR", true))
    );

    private final ApplicationInbox inbox;
    private final RestClient client;
    private final VacancyTraceStore store;
    private volatile boolean resetting = false;

    @Autowired
    public HiringTraceWorker(ApplicationInbox inbox, RestClient client, VacancyTraceStore store) {
        this.inbox = inbox;
        this.client = client;
        this.store = store;
    }

    HiringTraceWorker(ApplicationInbox inbox, RestClient client) {
        this(inbox, client, null);
    }

    @Scheduled(fixedDelayString = "${camunda.worker.poll-interval:1000}")
    void scheduledPoll() {
        if (!Settings.localCamundaSimulator() && !resetting) {
            if (store != null && store.isAutoApproveTasks()) {
                autoCompleteUserTasks();
            }
            poll();
        }
    }

    public void setResetting(boolean resetting) {
        this.resetting = resetting;
    }

    void poll() {
        try {
            JsonNode response = client.post().uri("/jobs/activation").body(Map.of(
                    "type", JOB_TYPE,
                    "worker", "flink-hiring-trace-collector",
                    "timeout", 30_000,
                    "maxJobsToActivate", 10,
                    "requestTimeout", -1,
                    "fetchVariable", List.of("vacancyId", "eventName", "role", "terminal")
            )).retrieve().body(JsonNode.class);
            if (response != null) {
                for (JsonNode job : response.path("jobs")) {
                    forward(job);
                }
            }
        } catch (RestClientException | IllegalArgumentException error) {
            log.warn("Hiring trace worker poll skipped: {}", error.getClass().getSimpleName());
        }
    }

    void autoCompleteUserTasks() {
        try {
            JsonNode response = client.post().uri("/user-tasks/search")
                    .body(Map.of("filter", Map.of("state", "CREATED")))
                    .retrieve()
                    .body(JsonNode.class);
            if (response == null) {
                return;
            }
            for (JsonNode task : response.path("items")) {
                String taskKey = task.path("userTaskKey").asText("");
                String name = task.path("name").asText("user-task");
                if (!taskKey.isBlank()) {
                    client.post().uri("/user-tasks/{key}/completion", taskKey)
                            .body(Map.of("variables", Map.of()))
                            .retrieve()
                            .toBodilessEntity();
                    log.info("Auto-approved Camunda user task userTaskKey={} name='{}'", taskKey, name);
                }
            }
        } catch (RestClientException | IllegalArgumentException error) {
            log.debug("Auto-approve user task poll skipped: {}", error.getMessage());
        }
    }

    public int cancelActiveProcessInstances() {
        if (Settings.localCamundaSimulator()) {
            return 0;
        }
        int cancelled = 0;
        try {
            for (int page = 0; page < 10; page++) {
                JsonNode response = client.post().uri("/process-instances/search")
                        .body(Map.of("filter", Map.of("state", "ACTIVE")))
                        .retrieve()
                        .body(JsonNode.class);
                JsonNode items = response != null ? response.path("items") : null;
                if (items == null || !items.isArray() || items.isEmpty()) {
                    break;
                }
                int cancelledInBatch = 0;
                for (JsonNode instance : items) {
                    String key = instance.path("processInstanceKey").asText("");
                    if (!key.isBlank()) {
                        try {
                            client.post().uri("/process-instances/{key}/cancellation", key)
                                    .body(Map.of())
                                    .retrieve()
                                    .toBodilessEntity();
                            cancelled++;
                            cancelledInBatch++;
                        } catch (RestClientException ignored) {
                            // Instance may have already completed or been cancelled by parent
                        }
                    }
                }
                if (cancelledInBatch == 0) {
                    break;
                }
            }
        } catch (RestClientException | IllegalArgumentException error) {
            log.warn("Could not cancel active Camunda instances on reset: {}", error.getMessage());
        }
        return cancelled;
    }

    private void forward(JsonNode job) {
        String jobKey = job.required("jobKey").asText();
        JsonNode vars = job.path("variables");
        JsonNode headers = job.path("customHeaders");
        String elementId = job.path("elementId").asText("");

        String vacancyId = textOrFallback(vars, "vacancyId", "VAC-" + job.path("processInstanceKey").asText("unknown"));
        ElementMeta mapped = FULL_BPMN_ELEMENTS.get(elementId);

        String eventName;
        String role;
        boolean terminal;
        if (mapped != null) {
            eventName = mapped.eventName();
            role = mapped.role();
            terminal = mapped.terminal();
        } else {
            eventName = textOrFallback(headers, "eventName", textOrFallback(vars, "eventName", "Unknown event"));
            role = textOrFallback(headers, "role", textOrFallback(vars, "role", "HR"));
            terminal = Boolean.parseBoolean(textOrFallback(headers, "terminal",
                    textOrFallback(vars, "terminal", "false")));
        }

        long now = System.currentTimeMillis();
        inbox.offer(new IntakeEvent<>(EventType.HIRING_TRACE_EVENT,
                new HiringTraceEvent(vacancyId, eventName, role, now, terminal)));

        // When "Request assessment of candidates" fires across the message flow to the
        // External Assessment Committee pool, emit the committee pool's 2 milestones in order.
        if ("Activity_1ynuleo".equals(elementId)) {
            inbox.offer(new IntakeEvent<>(EventType.HIRING_TRACE_EVENT,
                    new HiringTraceEvent(vacancyId, "Assess selected candidates", "External Assessment Committee", now + 1, false)));
            inbox.offer(new IntakeEvent<>(EventType.HIRING_TRACE_EVENT,
                    new HiringTraceEvent(vacancyId, "Inform DTU of selected candidates", "External Assessment Committee", now + 2, false)));
        }

        client.post().uri("/jobs/{key}/completion", jobKey)
                .body(Map.of("variables", Map.of("traceEventRecorded", true)))
                .retrieve()
                .toBodilessEntity();
        log.info("Forwarded Camunda hiring trace event vacancyId={} elementId={} event='{}' role='{}' terminal={}",
                vacancyId, elementId, eventName, role, terminal);
    }

    private static String textOrFallback(JsonNode node, String field, String fallback) {
        JsonNode value = node.path(field);
        if (value.isMissingNode() || value.isNull()) {
            return fallback;
        }
        String text = value.asText();
        return text.isBlank() ? fallback : text;
    }
}
