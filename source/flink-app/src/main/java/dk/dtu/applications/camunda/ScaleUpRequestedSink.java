package dk.dtu.applications.camunda;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import dk.dtu.applications.config.CamundaClientFactory;
import dk.dtu.applications.config.Settings;
import dk.dtu.applications.flink.RuntimeRegistry;
import dk.dtu.applications.model.ScaleUpRequested;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

@Component
public final class ScaleUpRequestedSink implements Sink<ScaleUpRequested> {
    private final CamundaClientFactory factory;
    private final String runtimeId;

    @Autowired
    public ScaleUpRequestedSink(CamundaClientFactory factory) {
        this(factory, null);
    }

    public ScaleUpRequestedSink(CamundaClientFactory factory, String runtimeId) {
        this.factory = factory;
        this.runtimeId = runtimeId;
    }

    public ScaleUpRequestedSink withRuntimeId(String runtimeId) {
        return new ScaleUpRequestedSink(factory, runtimeId);
    }

    @Override public SinkWriter<ScaleUpRequested> createWriter(WriterInitContext context) {
        return new Writer(new CamundaSink(factory.create()), runtimeId);
    }

    private static class Writer implements SinkWriter<ScaleUpRequested> {
        private final CamundaSink camunda;
        private final String runtimeId;

        Writer(CamundaSink camunda, String runtimeId) {
            this.camunda = camunda;
            this.runtimeId = runtimeId;
        }

        @Override public void write(ScaleUpRequested event, Context context) {
            if (runtimeId != null) {
                RuntimeRegistry.Services services = RuntimeRegistry.get(runtimeId);
                if (services != null && services.store() != null) {
                    services.store().recordScaleUpAlert(event.application, event.count);
                }
            }
            camunda.startProcess(event.application, event.count,
                Settings.value("SCALE_PROCESS_ID", "scale-up-application-receival"), ":scale");
        }

        @Override public void flush(boolean endOfInput) {}
        @Override public void close() {}
    }
}
