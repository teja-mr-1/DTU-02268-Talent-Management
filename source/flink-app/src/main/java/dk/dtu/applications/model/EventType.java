package dk.dtu.applications.model;

public enum EventType {
    APPLICATION(Application.class),
    SCALE_UP_COMPLETED(ScaleUpCompletion.class),
    HIRING_TRACE_EVENT(HiringTraceEvent.class);

    private final Class<?> payloadType;
    EventType(Class<?> payloadType) { this.payloadType = payloadType; }
    public Class<?> payloadType() { return payloadType; }
}
