package dk.dtu.applications.dcr;

import dk.dtu.applications.model.Distribution;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.springframework.stereotype.Component;

@Component
public record DistributionSink(DcrClient client) implements Sink<Distribution> {
    @Override public SinkWriter<Distribution> createWriter(WriterInitContext context) {
        return new SinkWriter<>() {
            @Override public void write(Distribution distribution, Context context) {
                client.executeEvent(distribution.simulationId, "A12");
            }

            @Override public void flush(boolean endOfInput) {}
            @Override public void close() {}
        };
    }
}
