package dk.dtu.applications.model;

public class ApplicationReceived {
    public Application application;
    public long count;
    public ApplicationReceived() {}
    public ApplicationReceived(Application application, long count) {
        this.application = application;
        this.count = count;
    }
}
