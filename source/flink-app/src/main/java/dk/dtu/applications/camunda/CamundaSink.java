package dk.dtu.applications.camunda;

import dk.dtu.applications.model.Application;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import java.util.Map;

public final class CamundaSink {
    private static final Logger log = LoggerFactory.getLogger(CamundaSink.class);
    private final RestClient client;

    public CamundaSink(RestClient client) { this.client = client; }

    public void startProcess(Application application, long count, String process, String suffix) {
        Map<String, Object> payload = Map.of("processDefinitionId", process, "variables", Map.of(
            "applicationId", application.applicationId,
            "name", application.name,
            "position", application.position,
            "receivedAt", application.receivedAt,
            "applicationsInLastHour", count,
            "triggerId", application.applicationId + suffix));
        log.debug("Sending Camunda process request applicationId={} processId={}", application.applicationId, process);
        try {
            client.post().uri("/process-instances").body(payload)
                .exchange((request, response) -> {
                    int status = response.getStatusCode().value();
                    if (status >= 200 && status < 300) {
                        log.info("Camunda process started applicationId={} processId={} status={}",
                            application.applicationId, process, status);
                    } else {
                        log.error("Camunda process request rejected applicationId={} processId={} status={}",
                            application.applicationId, process, status);
                    }
                    return null;
                });
        } catch (RestClientException failure) {
            log.error("Camunda request failed applicationId={} processId={}", application.applicationId, process, failure);
            throw failure;
        }
    }
}
