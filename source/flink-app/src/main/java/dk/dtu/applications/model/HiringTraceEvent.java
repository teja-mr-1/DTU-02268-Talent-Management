package dk.dtu.applications.model;

/**
 * Trace event emitted per vacancy from Camunda (or Event Source 2) into Flink.
 */
public class HiringTraceEvent {
    public String vacancyId;
    public String eventName;
    public String role;
    public long timestampEpochMs;
    public boolean terminal;

    public HiringTraceEvent() {
    }

    public HiringTraceEvent(String vacancyId, String eventName, String role, long timestampEpochMs, boolean terminal) {
        this.vacancyId = vacancyId;
        this.eventName = eventName;
        this.role = role;
        this.timestampEpochMs = timestampEpochMs;
        this.terminal = terminal;
    }
}
