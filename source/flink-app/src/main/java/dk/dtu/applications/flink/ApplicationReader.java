package dk.dtu.applications.flink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dk.dtu.applications.model.EventType;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.core.io.InputStatus;
import java.util.List;
import java.util.concurrent.CompletableFuture;

final class ApplicationReader<T> implements SourceReader<T, ApplicationSource.EmptyState> {
    private static final Logger log = LoggerFactory.getLogger(ApplicationReader.class);
    private final ApplicationInbox inbox;
    private final EventType key;
    private final Class<T> payloadType;
    ApplicationReader(ApplicationInbox inbox, EventType key, Class<T> payloadType) {
        this.inbox = inbox;
        this.key = key;
        this.payloadType = payloadType;
    }

    @Override public void start() {
        inbox.startReading(key);
        log.info("Flink inbox reader started key={}", key);
    }
    @Override public void addSplits(List<ApplicationSource.EmptyState> splits) {}
    @Override public InputStatus pollNext(ReaderOutput<T> output) {
        T payload = inbox.poll(key, payloadType);
        if (payload == null) return InputStatus.NOTHING_AVAILABLE;
        log.debug("Emitting inbox payload to Flink key={}", key);
        output.collect(payload);
        return InputStatus.MORE_AVAILABLE;
    }
    @Override public CompletableFuture<Void> isAvailable() { return inbox.available(key); }
    @Override public List<ApplicationSource.EmptyState> snapshotState(long checkpointId) { return List.of(); }
    @Override public void notifyNoMoreSplits() {}
    @Override public void close() {
        inbox.stopReading(key);
        log.info("Flink inbox reader stopped key={}", key);
    }
}
