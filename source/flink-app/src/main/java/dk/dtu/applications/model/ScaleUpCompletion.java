package dk.dtu.applications.model;

/** Correlation data returned by the final scale-up service task. */
public class ScaleUpCompletion {
    public String processInstanceKey;
    public String applicationId;
    public String triggerId;
    public long receivedAt;
    public ScaleUpCompletion() {}
}
