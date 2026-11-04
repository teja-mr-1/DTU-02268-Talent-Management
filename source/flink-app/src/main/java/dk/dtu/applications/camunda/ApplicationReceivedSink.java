package dk.dtu.applications.camunda;

import org.springframework.stereotype.Component;
import dk.dtu.applications.config.CamundaClientFactory;
import dk.dtu.applications.config.Settings;
import dk.dtu.applications.model.ApplicationReceived;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

@Component
public final class ApplicationReceivedSink implements Sink<ApplicationReceived> {
    private final CamundaClientFactory factory;

    public ApplicationReceivedSink(CamundaClientFactory factory) { this.factory = factory; }

    @Override public SinkWriter<ApplicationReceived> createWriter(WriterInitContext context) {
        return new Writer(new CamundaSink(factory.create()));
    }

    private static class Writer implements SinkWriter<ApplicationReceived> {
        private final CamundaSink camunda;

        Writer(CamundaSink camunda) { this.camunda = camunda; }

        @Override public void write(ApplicationReceived event, Context context) {
            camunda.startProcess(event.application, event.count,
                Settings.value("APPLICATION_PROCESS_ID", "application-received"), ":application");
        }

        @Override public void flush(boolean endOfInput) {}
        @Override public void close() {}
    }
}
