package dk.dtu.applications.flink;

import dk.dtu.applications.config.Settings;
import dk.dtu.applications.model.ConformanceResult;
import dk.dtu.applications.model.HiringTraceEvent;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory view of per-vacancy trace events, generated XES logs,
 * and Process Doctor DCR conformance verdicts.
 */
@Component
public final class VacancyTraceStore {
    private final ConcurrentHashMap<String, VacancyRecord> vacancies = new ConcurrentHashMap<>();
    private final boolean autoConformanceCheck;

    public VacancyTraceStore() {
        this(Boolean.parseBoolean(Settings.value("AUTO_CONFORMANCE_CHECK", "true")));
    }

    VacancyTraceStore(boolean autoConformanceCheck) {
        this.autoConformanceCheck = autoConformanceCheck;
    }

    public boolean isAutoConformanceCheck() {
        return autoConformanceCheck;
    }

    public void recordTraceEvent(String vacancyId, List<HiringTraceEvent> allEventsSoFar) {
        VacancyRecord record = vacancies.computeIfAbsent(vacancyId, VacancyRecord::new);
        synchronized (record) {
            record.events = new ArrayList<>(allEventsSoFar);
            record.status = "COLLECTING";
            record.errorMessage = null;
            record.updatedAt = Instant.now();
        }
    }

    public void markChecking(String vacancyId, List<HiringTraceEvent> finalEvents, String xesPath, String xesXml) {
        VacancyRecord record = vacancies.computeIfAbsent(vacancyId, VacancyRecord::new);
        synchronized (record) {
            if (finalEvents != null && !finalEvents.isEmpty()) {
                record.events = new ArrayList<>(finalEvents);
            }
            record.xesPath = xesPath;
            record.xesXml = xesXml;
            record.status = "CHECKING_CONFORMANCE";
            record.conformanceResult = null;
            record.errorMessage = null;
            record.updatedAt = Instant.now();
        }
    }

    public void markReadyForManualUpload(String vacancyId) {
        VacancyRecord record = vacancies.computeIfAbsent(vacancyId, VacancyRecord::new);
        synchronized (record) {
            record.status = "READY_FOR_MANUAL_UPLOAD";
            record.updatedAt = Instant.now();
        }
    }

    public void recordConformanceResult(String vacancyId, ConformanceResult result) {
        VacancyRecord record = vacancies.computeIfAbsent(vacancyId, VacancyRecord::new);
        synchronized (record) {
            record.conformanceResult = result;
            record.status = result.conformant() ? "CONFORMANT" : "VIOLATING";
            record.errorMessage = null;
            record.updatedAt = Instant.now();
        }
    }

    public void recordConformanceError(String vacancyId, String errorMessage) {
        VacancyRecord record = vacancies.computeIfAbsent(vacancyId, VacancyRecord::new);
        synchronized (record) {
            record.status = "ERROR";
            record.errorMessage = errorMessage;
            record.updatedAt = Instant.now();
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

    public String getXesPath(String vacancyId) {
        VacancyRecord record = vacancies.get(vacancyId);
        if (record == null) {
            return null;
        }
        synchronized (record) {
            return record.xesPath;
        }
    }

    public List<Map<String, Object>> snapshotVacancies() {
        List<Map<String, Object>> list = new ArrayList<>();
        vacancies.values().stream()
                .sorted((a, b) -> b.updatedAt.compareTo(a.updatedAt))
                .forEach(record -> list.add(toMap(record)));
        return list;
    }

    public Optional<Map<String, Object>> snapshotVacancy(String vacancyId) {
        VacancyRecord record = vacancies.get(vacancyId);
        return record == null ? Optional.empty() : Optional.of(toMap(record));
    }

    private static Map<String, Object> toMap(VacancyRecord record) {
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
            m.put("updatedAt", record.updatedAt);
            return m;
        }
    }

    private static final class VacancyRecord {
        final String vacancyId;
        List<HiringTraceEvent> events = new ArrayList<>();
        String status = "COLLECTING";
        String xesPath;
        String xesXml;
        ConformanceResult conformanceResult;
        String errorMessage;
        Instant updatedAt = Instant.now();

        VacancyRecord(String vacancyId) {
            this.vacancyId = vacancyId;
        }
    }
}
