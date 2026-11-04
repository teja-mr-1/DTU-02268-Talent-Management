package dk.dtu.applications.flink;

import org.springframework.stereotype.Component;
import dk.dtu.applications.model.EventType;
import dk.dtu.applications.model.IntakeEvent;
import java.util.ArrayDeque;
import java.util.EnumMap;
import java.util.concurrent.CompletableFuture;

@Component
public final class ApplicationInbox {
    private final EnumMap<EventType, Channel> channels = new EnumMap<>(EventType.class);

    public ApplicationInbox() {
        for (EventType type : EventType.values()) channels.put(type, new Channel());
    }

    public synchronized void offer(IntakeEvent<?> event) {
        Channel channel = channels.get(event.type());
        channel.queue.add(event);
        channel.available.complete(null);
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
        CompletableFuture<Void> available = new CompletableFuture<>();
    }
}
