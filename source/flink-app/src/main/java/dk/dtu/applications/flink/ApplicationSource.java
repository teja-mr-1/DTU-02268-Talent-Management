package dk.dtu.applications.flink;

import dk.dtu.applications.model.EventType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.connector.testframe.source.enumerator.NoOpEnumerator;
import org.apache.flink.connector.testframe.source.enumerator.NoOpEnumState;
import org.apache.flink.connector.testframe.source.enumerator.NoOpEnumStateSerializer;
import org.apache.flink.connector.testframe.source.split.FromElementsSplit;
import org.apache.flink.connector.testframe.source.split.FromElementsSplitSerializer;

public class ApplicationSource<T> implements Source<T, FromElementsSplit, NoOpEnumState>, ResultTypeQueryable<T> {
    private final String inboxId;
    private final EventType key;
    private final Class<T> payloadType;
    public ApplicationSource(String inboxId, EventType key, Class<T> payloadType) {
        if (key.payloadType() != payloadType) throw new IllegalArgumentException("Incorrect payload type for " + key);
        this.inboxId = inboxId;
        this.key = key;
        this.payloadType = payloadType;
    }
    @Override public TypeInformation<T> getProducedType() { return TypeInformation.of(payloadType); }
    @Override public Boundedness getBoundedness() { return Boundedness.CONTINUOUS_UNBOUNDED; }
    @Override public SourceReader<T, FromElementsSplit> createReader(SourceReaderContext context) {
        return new ApplicationReader<>(InboxRegistry.get(inboxId), key, payloadType);
    }
    @Override public SplitEnumerator<FromElementsSplit, NoOpEnumState> createEnumerator(SplitEnumeratorContext<FromElementsSplit> context) {
        return enumerator(context);
    }
    @Override public SplitEnumerator<FromElementsSplit, NoOpEnumState> restoreEnumerator(
            SplitEnumeratorContext<FromElementsSplit> context, NoOpEnumState ignored) {
        return enumerator(context);
    }

    private SplitEnumerator<FromElementsSplit, NoOpEnumState> enumerator(SplitEnumeratorContext<FromElementsSplit> context) {
        if (context.currentParallelism() != 1) throw new IllegalArgumentException("Embedded intake requires parallelism 1");
        return new NoOpEnumerator();
    }

    @Override public SimpleVersionedSerializer<FromElementsSplit> getSplitSerializer() {
        return new FromElementsSplitSerializer();
    }
    @Override public SimpleVersionedSerializer<NoOpEnumState> getEnumeratorCheckpointSerializer() {
        return new NoOpEnumStateSerializer();
    }
}
