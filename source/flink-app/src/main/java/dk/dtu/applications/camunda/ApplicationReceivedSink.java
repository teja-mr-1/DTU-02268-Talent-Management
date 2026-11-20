package dk.dtu.applications.camunda;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import dk.dtu.applications.config.CamundaClientFactory;
import dk.dtu.applications.config.Settings;
import dk.dtu.applications.flink.RuntimeRegistry;
import dk.dtu.applications.model.ApplicationReceived;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

@Component
public final class ApplicationReceivedSink implements Sink<ApplicationReceived> {
    private final CamundaClientFactory factory;
    private final String runtimeId;

    @Autowired
    public ApplicationReceivedSink(CamundaClientFactory factory) {
        this(factory, null);
    }

    public ApplicationReceivedSink(CamundaClientFactory factory, String runtimeId) {
        this.factory = factory;
        this.runtimeId = runtimeId;
    }

    public ApplicationReceivedSink withRuntimeId(String runtimeId) {
        return new ApplicationReceivedSink(factory, runtimeId);
    }

    @Override public SinkWriter<ApplicationReceived> createWriter(WriterInitContext context) {
        return new Writer(new CamundaSink(factory.create()), runtimeId);
    }

    private static class Writer implements SinkWriter<ApplicationReceived> {
        private final CamundaSink camunda;
        private final String runtimeId;

        Writer(CamundaSink camunda, String runtimeId) {
            this.camunda = camunda;
            this.runtimeId = runtimeId;
        }

        @Override public void write(ApplicationReceived event, Context context) {
            String hiringProcessId = Settings.value("APPLICATION_PROCESS_ID", "application-received");
            if (runtimeId == null) {
                camunda.startProcess(event.application, event.count, hiringProcessId, ":application");
                return;
            }
            RuntimeRegistry.Services services = RuntimeRegistry.get(runtimeId);
            String vacancyId = (event.application.vacancyId == null || event.application.vacancyId.isBlank())
                    ? "VAC-FP-2026-01"
                    : event.application.vacancyId;

            if (services != null && services.store() != null) {
                services.store().recordApplication(event.application, event.count);
            }

            // Always start per-applicant intake ("application-received")
            camunda.startProcess(event.application, event.count, "application-received", ":application");

            // Start the full per-vacancy hiring process ("hiring-full-professor") only ONCE per vacancy
            if (!"application-received".equals(hiringProcessId)
                    && (services == null || services.store() == null
                    || services.store().tryClaimVacancyHiringStart(vacancyId))) {
                camunda.startProcess(event.application, event.count, hiringProcessId, ":hiring");
            }
        }

        @Override public void flush(boolean endOfInput) {}
        @Override public void close() {}
    }
}
