package dk.dtu.applications.flink;

import dk.dtu.applications.dcr.ScaleUpProcessState;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Lets serialized Flink sources, operators, and sinks look up the original Spring beans
 * by ID instead of serializing or copying them. This keeps the application inbox and
 * scale-up state shared with the HTTP layer. Requires embedded Flink in the same JVM.
 */
public final class FlinkRegistry {
    private static final ConcurrentHashMap<String, ApplicationInbox> INBOXES = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, ScaleUpProcessState> SCALE_UP_STATES = new ConcurrentHashMap<>();
    private FlinkRegistry() {}

    public static String register(ApplicationInbox inbox) {
        String id = UUID.randomUUID().toString();
        INBOXES.put(id, inbox);
        return id;
    }
    public static String register(ApplicationInbox inbox, ScaleUpProcessState state) {
        String id = register(inbox);
        SCALE_UP_STATES.put(id, state);
        return id;
    }

    public static ScaleUpProcessState getScaleUpState(String id) {
        ScaleUpProcessState state = SCALE_UP_STATES.get(id);
        if (state == null) throw new IllegalStateException("Scale-up state not found for embedded runtime");
        return state;
    }

    static ApplicationInbox getApplicationInbox(String id) {
        ApplicationInbox inbox = INBOXES.get(id);
        if (inbox == null) throw new IllegalStateException("Inbox not found; this source requires local embedded execution");
        return inbox;
    }
    public static void remove(String id) {
        INBOXES.remove(id);
        SCALE_UP_STATES.remove(id);
    }
}
