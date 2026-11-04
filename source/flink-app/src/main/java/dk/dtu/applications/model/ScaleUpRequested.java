package dk.dtu.applications.model;

public class ScaleUpRequested {
    public Application application;
    public long count;
    public ScaleUpRequested() {}
    public ScaleUpRequested(Application application, long count) {
        this.application = application;
        this.count = count;
    }
}
