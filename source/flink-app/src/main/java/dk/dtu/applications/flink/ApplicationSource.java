package dk.dtu.applications.flink;

import dk.dtu.applications.model.EventType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import java.io.IOException;
import java.util.List;

public class ApplicationSource<T> implements Source<T, ApplicationSource.EmptyState, ApplicationSource.EmptyState>, ResultTypeQueryable<T> {
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
    @Override public SourceReader<T, EmptyState> createReader(SourceReaderContext context) {
        return new ApplicationReader<>(InboxRegistry.get(inboxId), key, payloadType);
    }
    @Override public SplitEnumerator<EmptyState, EmptyState> createEnumerator(SplitEnumeratorContext<EmptyState> context) {
        return enumerator(context);
    }
    @Override public SplitEnumerator<EmptyState, EmptyState> restoreEnumerator(
            SplitEnumeratorContext<EmptyState> context, EmptyState ignored) {
        return enumerator(context);
    }
    public enum EmptyState implements SourceSplit {
        INSTANCE;
        @Override public String splitId() { return "unused"; }
    }

    private SplitEnumerator<EmptyState, EmptyState> enumerator(SplitEnumeratorContext<EmptyState> context) {
        if (context.currentParallelism() != 1) throw new IllegalArgumentException("Embedded intake requires parallelism 1");
        return new SplitEnumerator<>() {
            @Override public void start() {}
            @Override public void addReader(int reader) {}
            @Override public void handleSplitRequest(int reader, String host) {}
            @Override public void addSplitsBack(List<EmptyState> splits, int reader) {}
            @Override public EmptyState snapshotState(long checkpointId) { return EmptyState.INSTANCE; }
            @Override public void close() {}
        };
    }

    @Override public SimpleVersionedSerializer<EmptyState> getSplitSerializer() {
        return getEnumeratorCheckpointSerializer();
    }
    @Override public SimpleVersionedSerializer<EmptyState> getEnumeratorCheckpointSerializer() {
        return new SimpleVersionedSerializer<>() {
            @Override public int getVersion() { return 1; }
            @Override public byte[] serialize(EmptyState state) { return new byte[0]; }
            @Override public EmptyState deserialize(int version, byte[] bytes) throws IOException {
                if (version != 1 || bytes.length != 0) throw new IOException("Invalid empty source state");
                return EmptyState.INSTANCE;
            }
        };
    }
}
