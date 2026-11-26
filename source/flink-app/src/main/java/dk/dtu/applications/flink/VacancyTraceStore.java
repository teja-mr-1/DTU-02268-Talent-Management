package dk.dtu.applications.flink;

import dk.dtu.applications.model.ConformanceResult;
import dk.dtu.applications.model.HiringTraceEvent;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public final class VacancyTraceStore {
    private final ConcurrentHashMap<String, VacancyRecord> vacancies = new ConcurrentHashMap<>();

    public void recordTraceEvent(String vacancyId, List<HiringTraceEvent> events) {
        VacancyRecord record = vacancies.computeIfAbsent(vacancyId, VacancyRecord::new);
        synchronized (record) {
            record.events = new ArrayList<>(events);
            record.status = "COLLECTING";
            record.errorMessage = null;
        }
    }

    public void markChecking(String vacancyId, List<HiringTraceEvent> events, String xesPath, String xesXml) {
        VacancyRecord record = vacancies.computeIfAbsent(vacancyId, VacancyRecord::new);
        synchronized (record) {
            record.events = new ArrayList<>(events);
            record.xesPath = xesPath;
            record.xesXml = xesXml;
            record.status = "CHECKING";
            record.conformanceResult = null;
            record.errorMessage = null;
        }
    }

    public void markXesReady(String vacancyId) {
        VacancyRecord record = vacancies.computeIfAbsent(vacancyId, VacancyRecord::new);
        synchronized (record) {
            record.status = "XES_READY";
        }
    }

    public void recordConformanceResult(String vacancyId, ConformanceResult result) {
        VacancyRecord record = vacancies.computeIfAbsent(vacancyId, VacancyRecord::new);
        synchronized (record) {
            record.conformanceResult = result;
            record.status = result.conformant() ? "CONFORMANT" : "VIOLATING";
            record.errorMessage = null;
        }
    }

    public void recordConformanceError(String vacancyId, String errorMessage) {
        VacancyRecord record = vacancies.computeIfAbsent(vacancyId, VacancyRecord::new);
        synchronized (record) {
            record.status = "ERROR";
            record.errorMessage = errorMessage;
        }
    }

    public String getXesXml(String vacancyId) {
        VacancyRecord record = vacancies.get(vacancyId);
        if (record == null) {
            return null;
        }
        synchronized (record) {
            return record.xesXml;
        }
    }

    public List<Map<String, Object>> snapshotVacancies() {
        List<Map<String, Object>> list = new ArrayList<>();
        for (VacancyRecord record : vacancies.values()) {
            synchronized (record) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("vacancyId", record.vacancyId);
                m.put("status", record.status);
                m.put("eventCount", record.events.size());
                m.put("events", record.events.stream().map(e -> Map.of(
                        "eventName", e.eventName,
                        "role", e.role,
                        "timestamp", Instant.ofEpochMilli(e.timestampEpochMs).toString(),
                        "terminal", e.terminal
                )).toList());
                m.put("xesPath", record.xesPath);
                m.put("conformanceResult", record.conformanceResult);
                m.put("errorMessage", record.errorMessage);
                list.add(m);
            }
        }
        return list;
    }

    private static final class VacancyRecord {
        final String vacancyId;
        List<HiringTraceEvent> events = new ArrayList<>();
        String status = "COLLECTING";
        String xesPath;
        String xesXml;
        ConformanceResult conformanceResult;
        String errorMessage;

        VacancyRecord(String vacancyId) {
            this.vacancyId = vacancyId;
        }
    }
}
