package dk.dtu.applications.dcr;

import org.springframework.stereotype.Component;
import dk.dtu.applications.flink.FlinkRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import dk.dtu.applications.model.ScaleUpRequested;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

@Component
public final class ScaleUpRequestedSink implements Sink<ScaleUpRequested> {
    private static final Logger log = LoggerFactory.getLogger(ScaleUpRequestedSink.class);
    private final DcrClient client;
    private final transient ScaleUpProcessState state;

    private String inboxId;

    public ScaleUpRequestedSink(DcrClient client, ScaleUpProcessState state) {
        this.client = client;
        this.state = state;
    }

    public ScaleUpProcessState state() { return state; }

    public ScaleUpRequestedSink forInbox(String inboxId) {
        var sink = new ScaleUpRequestedSink(client, state);
        sink.inboxId = inboxId;
        return sink;
    }

    @Override public SinkWriter<ScaleUpRequested> createWriter(WriterInitContext context) {
        return new Writer(client, inboxId == null ? state : FlinkRegistry.getScaleUpState(inboxId));
    }

    private static class Writer implements SinkWriter<ScaleUpRequested> {
        private final DcrClient client;
        private final ScaleUpProcessState state;

        Writer(DcrClient client, ScaleUpProcessState state) { this.client = client; this.state = state; }

        @Override public void write(ScaleUpRequested event, Context context) {
            synchronized (state) {
                if (state.simulationId() != null) {
                    log.info("Skipping scale-up: simulation already active simulationId={}", state.simulationId());
                    return;
                }
                try {
                    client.startProcess();
                    state.setSimulationId(client.getNewestProcessId());
                    log.info("Scale-up process started simulationId={}", state.simulationId());
                } catch (RuntimeException failure) {
                    log.error("DCR scale-up request failed", failure);
                    throw failure;
                }
            }
        }

        @Override public void flush(boolean endOfInput) {}
        @Override public void close() {}
    }
}
