package dk.dtu.applications.model;

/**
 * Emitted by Flink once a vacancy reaches a terminal event and its XES trace log is published.
 */
public class VacancyLogReady {
    public String vacancyId;
    public String xesPath;
    public String xesXml;
    public int eventCount;

    public VacancyLogReady() {
    }

    public VacancyLogReady(String vacancyId, String xesPath, String xesXml, int eventCount) {
        this.vacancyId = vacancyId;
        this.xesPath = xesPath;
        this.xesXml = xesXml;
        this.eventCount = eventCount;
    }
}
