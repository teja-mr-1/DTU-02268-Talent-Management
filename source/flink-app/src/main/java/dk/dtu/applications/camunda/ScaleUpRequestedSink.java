package dk.dtu.applications.camunda;

import org.springframework.stereotype.Component;
import dk.dtu.applications.config.CamundaClientFactory;
import dk.dtu.applications.config.Settings;
import dk.dtu.applications.model.ScaleUpRequested;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

@Component
public final class ScaleUpRequestedSink implements Sink<ScaleUpRequested> {
    private final CamundaClientFactory factory;

    public ScaleUpRequestedSink(CamundaClientFactory factory) { this.factory = factory; }

    @Override public SinkWriter<ScaleUpRequested> createWriter(WriterInitContext context) {
        return new Writer(new CamundaSink(factory.create()));
    }

    private static class Writer implements SinkWriter<ScaleUpRequested> {
        private final CamundaSink camunda;

        Writer(CamundaSink camunda) { this.camunda = camunda; }

        @Override public void write(ScaleUpRequested event, Context context) {
            camunda.startProcess(event.application, event.count,
                Settings.value("SCALE_PROCESS_ID", "scale-up-application-receival"), ":scale");
        }

        @Override public void flush(boolean endOfInput) {}
        @Override public void close() {}
    }
}
