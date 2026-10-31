package dk.dtu.applications.flink;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public final class InboxRegistry {
    private static final ConcurrentHashMap<String, ApplicationInbox> INBOXES = new ConcurrentHashMap<>();
    private InboxRegistry() {}

    public static String register(ApplicationInbox inbox) {
        String id = UUID.randomUUID().toString();
        INBOXES.put(id, inbox);
        return id;
    }
    static ApplicationInbox get(String id) {
        ApplicationInbox inbox = INBOXES.get(id);
        if (inbox == null) throw new IllegalStateException("Inbox not found; this source requires local embedded execution");
        return inbox;
    }
    public static void remove(String id) { INBOXES.remove(id); }
}
