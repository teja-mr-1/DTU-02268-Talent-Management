package dk.dtu.applications.model;

public class Command {
    public Application application;
    public boolean scaleUp;
    public long count;
    public Command() {}
    public Command(Application application, boolean scaleUp, long count) {
        this.application = application; this.scaleUp = scaleUp; this.count = count;
    }
}
