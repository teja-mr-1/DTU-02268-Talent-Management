package dk.dtu.applications.model;

public enum EventType {
    APPLICATION(Application.class),
    HIRING_TRACE_EVENT(HiringTraceEvent.class);

    private final Class<?> payloadType;
    EventType(Class<?> payloadType) { this.payloadType = payloadType; }
    public Class<?> payloadType() { return payloadType; }
}
