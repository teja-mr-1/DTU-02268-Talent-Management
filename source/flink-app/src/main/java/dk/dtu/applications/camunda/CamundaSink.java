package dk.dtu.applications.camunda;

import dk.dtu.applications.config.CamundaClientFactory;
import dk.dtu.applications.config.Settings;
import dk.dtu.applications.model.Command;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import java.util.Map;

public class CamundaSink implements Sink<Command> {
    private static final Logger log = LoggerFactory.getLogger(CamundaSink.class);
    private final CamundaClientFactory factory;

    public CamundaSink(CamundaClientFactory factory) { this.factory = factory; }
    @Override public SinkWriter<Command> createWriter(WriterInitContext context) {
        return new Writer(factory.create());
    }

    private static class Writer implements SinkWriter<Command> {
        private final RestClient client;
        Writer(RestClient client) { this.client = client; }

        @Override public void write(Command command, Context context) {
            String process = command.scaleUp
                ? Settings.value("SCALE_PROCESS_ID", "scale-up-application-receival")
                : Settings.value("APPLICATION_PROCESS_ID", "application-received");
            log.debug("Sending Camunda process request applicationId={} processId={}", command.application.applicationId, process);
            try {
                client.post().uri("/process-instances").body(payload(command, process))
                    .exchange((request, response) -> {
                        int status = response.getStatusCode().value();
                        if (status >= 200 && status < 300) {
                            log.info("Camunda process started applicationId={} processId={} status={}",
                                command.application.applicationId, process, status);
                        } else {
                            log.error("Camunda process request rejected applicationId={} processId={} status={}",
                                command.application.applicationId, process, status);
                        }
                        return null;
                    });
            } catch (RestClientException failure) {
                log.error("Camunda request failed applicationId={} processId={}", command.application.applicationId, process, failure);
                throw failure;
            }
        }

        private Map<String, Object> payload(Command command, String process) {
            return Map.of("processDefinitionId", process, "variables", Map.of(
                "applicationId", command.application.applicationId,
                "name", command.application.name,
                "position", command.application.position,
                "receivedAt", command.application.receivedAt,
                "applicationsInLastHour", command.count,
                "triggerId", command.application.applicationId + (command.scaleUp ? ":scale" : ":application")));
        }

        @Override public void flush(boolean endOfInput) {}
        @Override public void close() {}
    }
}
