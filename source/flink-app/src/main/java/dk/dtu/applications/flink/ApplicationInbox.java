package dk.dtu.applications.flink;

import dk.dtu.applications.model.EventType;
import dk.dtu.applications.model.IntakeEvent;
import java.util.ArrayDeque;
import java.util.EnumMap;
import java.util.concurrent.CompletableFuture;

/** Each event key has its own bounded queue, reader readiness, and wake-up signal. */
public final class ApplicationInbox {
    private final EnumMap<EventType, Channel> channels = new EnumMap<>(EventType.class);
    private final int capacity;

    public ApplicationInbox(int capacity) {
        if (capacity < 1) throw new IllegalArgumentException("Inbox capacity must be positive");
        this.capacity = capacity;
        for (EventType type : EventType.values()) channels.put(type, new Channel());
    }

    public synchronized void startReading(EventType key) {
        Channel channel = channels.get(key);
        channel.reading = true;
        channel.started.complete(null);
    }
    public synchronized void stopReading(EventType key) { channels.get(key).reading = false; }
    public synchronized void stopReading() { channels.values().forEach(channel -> channel.reading = false); }
    public CompletableFuture<Void> started() {
        return CompletableFuture.allOf(channels.values().stream()
            .map(channel -> channel.started).toArray(CompletableFuture[]::new));
    }

    public synchronized boolean offer(IntakeEvent<?> event) {
        Channel channel = channels.get(event.type());
        if (!channel.reading || channel.queue.size() >= capacity) return false;
        channel.queue.add(event);
        channel.available.complete(null);
        return true;
    }

    public synchronized <T> T poll(EventType key, Class<T> payloadType) {
        if (key.payloadType() != payloadType) throw new IllegalArgumentException("Incorrect payload type for " + key);
        Channel channel = channels.get(key);
        IntakeEvent<?> event = channel.queue.poll();
        if (channel.queue.isEmpty() && channel.available.isDone()) channel.available = new CompletableFuture<>();
        return event == null ? null : payloadType.cast(event.payload());
    }
    public synchronized CompletableFuture<Void> available(EventType key) { return channels.get(key).available; }

    private static class Channel {
        final ArrayDeque<IntakeEvent<?>> queue = new ArrayDeque<>();
        final CompletableFuture<Void> started = new CompletableFuture<>();
        CompletableFuture<Void> available = new CompletableFuture<>();
        boolean reading;
    }
}
