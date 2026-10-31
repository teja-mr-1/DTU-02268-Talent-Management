package dk.dtu.applications.camunda;

import com.fasterxml.jackson.databind.JsonNode;
import dk.dtu.applications.flink.ApplicationInbox;
import dk.dtu.applications.model.IntakeEvent;
import dk.dtu.applications.model.EventType;
import dk.dtu.applications.model.ScaleUpCompletion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.util.List;
import java.util.Map;

@Component
@ConditionalOnProperty(name = "camunda.worker.enabled", matchIfMissing = true)
final class ScaleUpCompletionWorker {
    static final String JOB_TYPE = "notify-flink-scale-up-completed";
    private static final Logger log = LoggerFactory.getLogger(ScaleUpCompletionWorker.class);
    private final ApplicationInbox inbox;
    private final RestClient client;

    ScaleUpCompletionWorker(ApplicationInbox inbox, RestClient client) {
        this.inbox = inbox;
        this.client = client;
    }

    @Scheduled(fixedDelayString = "${camunda.worker.poll-interval:1000}")
    void poll() {
        try {
            JsonNode response = client.post().uri("/jobs/activation").body(Map.of(
                    "type", JOB_TYPE, "worker", "flink-scale-up-completion",
                    "timeout", 30_000, "maxJobsToActivate", 1, "requestTimeout", -1,
                    "fetchVariable", List.of("applicationId", "triggerId"))).retrieve().body(JsonNode.class);
            if (response != null) for (JsonNode job : response.path("jobs")) forward(job);
        } catch (RestClientException | IllegalArgumentException error) {
            // No local retry loop: an uncompleted activation becomes available after its lock expires.
            log.warn("Scale-up notification worker could not finish its poll: {}", error.getClass().getSimpleName());
        }
    }

    private void forward(JsonNode job) {
        String jobKey = job.required("jobKey").asText();
        ScaleUpCompletion completion = new ScaleUpCompletion();
        completion.processInstanceKey = job.required("processInstanceKey").asText();
        completion.applicationId = job.required("variables").required("applicationId").asText();
        completion.triggerId = job.required("variables").required("triggerId").asText();
        completion.receivedAt = System.currentTimeMillis();
        if (!inbox.offer(new IntakeEvent<>(EventType.SCALE_UP_COMPLETED, completion))) {
            log.warn("Scale-up completion deferred: inbox unavailable or full processInstanceKey={}", completion.processInstanceKey);
            return; // Do not acknowledge a notification we have not accepted.
        }
        client.post().uri("/jobs/{key}/completion", jobKey)
                .body(Map.of("variables", Map.of("flinkNotificationAccepted", true)))
                .retrieve().toBodilessEntity();
        log.info("Scale-up notification queued and service task completed processInstanceKey={} applicationId={}",
                completion.processInstanceKey, completion.applicationId);
    }

}
