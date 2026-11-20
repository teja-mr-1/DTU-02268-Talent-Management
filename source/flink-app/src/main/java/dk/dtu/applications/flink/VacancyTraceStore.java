package dk.dtu.applications.flink;

import dk.dtu.applications.config.Settings;
import dk.dtu.applications.model.Application;
import dk.dtu.applications.model.ConformanceResult;
import dk.dtu.applications.model.HiringTraceEvent;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory live view of per-vacancy traces, XES logs, Process Doctor results,
 * and Flink rolling-hour application/scale-up telemetry for the dashboard API.
 */
@Component
public final class VacancyTraceStore {
    private static final int MAX_RECENT_APPLICATIONS = 25;

    private final ConcurrentHashMap<String, VacancyRecord> vacancies = new ConcurrentHashMap<>();
    private final Set<String> startedHiringVacancies = ConcurrentHashMap.newKeySet();
    private final Deque<Map<String, Object>> recentApplications = new ArrayDeque<>();
    private final Deque<Map<String, Object>> scaleUpAlerts = new ArrayDeque<>();
    private final AtomicInteger totalApplications = new AtomicInteger();
    private final AtomicLong resetEpoch = new AtomicLong(0);
    private final AtomicBoolean autoApproveTasks = new AtomicBoolean(
            Boolean.parseBoolean(Settings.value("AUTO_APPROVE_TASKS", "false")));
    private final AtomicBoolean autoConformanceCheck = new AtomicBoolean(
            Boolean.parseBoolean(Settings.value("AUTO_CONFORMANCE_CHECK", "false")));

    public boolean isAutoApproveTasks() {
        return autoApproveTasks.get();
    }

    public void setAutoApproveTasks(boolean enabled) {
        autoApproveTasks.set(enabled);
    }

    public boolean isAutoConformanceCheck() {
        return autoConformanceCheck.get();
    }

    public void setAutoConformanceCheck(boolean enabled) {
        autoConformanceCheck.set(enabled);
    }

    public boolean tryClaimVacancyHiringStart(String vacancyId) {
        if (vacancyId == null || vacancyId.isBlank()) {
            return true;
        }
        return startedHiringVacancies.add(vacancyId);
    }

    public boolean isVacancyClosed(String vacancyId) {
        if (vacancyId == null || vacancyId.isBlank()) {
            return false;
        }
        VacancyRecord record = vacancies.get(vacancyId);
        if (record == null) {
            return false;
        }
        synchronized (record) {
            return "CONFORMANT".equals(record.status)
                    || "VIOLATING".equals(record.status)
                    || "CHECKING_CONFORMANCE".equals(record.status)
                    || "READY_FOR_MANUAL_UPLOAD".equals(record.status)
                    || record.events.stream().anyMatch(e -> e.terminal);
        }
    }

    public void recordApplication(Application app, long rollingHourCount) {
        totalApplications.incrementAndGet();
        String vid = (app.vacancyId == null || app.vacancyId.isBlank()) ? "VAC-FP-2026-01" : app.vacancyId;
        VacancyRecord record = vacancies.computeIfAbsent(vid, VacancyRecord::new);
        synchronized (record) {
            if (app.name != null && !app.name.isBlank()) {
                record.applicants.add(app.name);
                Map<String, Object> profile = new LinkedHashMap<>();
                profile.put("name", app.name);
                profile.put("email", app.email != null ? app.email : "applicant@dtu-demo.dk");
                profile.put("citizenship", app.citizenship != null ? app.citizenship : "Denmark (EU/EEA)");
                profile.put("previouslyAtDtu", app.previouslyAtDtu != null ? app.previouslyAtDtu : "No");
                profile.put("submittedAt", Instant.ofEpochMilli(app.receivedAt).toString());
                record.applicantProfiles.add(profile);
            }
            record.updatedAt = Instant.now();
        }
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("applicationId", app.applicationId);
        item.put("vacancyId", app.vacancyId);
        item.put("name", app.name);
        item.put("position", app.position);
        item.put("rollingHourCount", rollingHourCount);
        item.put("receivedAt", Instant.ofEpochMilli(app.receivedAt));
        synchronized (recentApplications) {
            recentApplications.addFirst(item);
            while (recentApplications.size() > MAX_RECENT_APPLICATIONS) {
                recentApplications.removeLast();
            }
        }
    }

    public void recordScaleUpAlert(Application app, long rollingHourCount) {
        Map<String, Object> alert = new LinkedHashMap<>();
        alert.put("applicationId", app.applicationId);
        alert.put("vacancyId", app.vacancyId);
        alert.put("applicationsInLastHour", rollingHourCount);
        alert.put("triggeredAt", Instant.now());
        synchronized (scaleUpAlerts) {
            scaleUpAlerts.addFirst(alert);
            while (scaleUpAlerts.size() > 10) {
                scaleUpAlerts.removeLast();
            }
        }
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

    public List<String> pendingManualUploadVacancyIds() {
        List<String> ids = new ArrayList<>();
        vacancies.forEach((id, record) -> {
            synchronized (record) {
                if ("READY_FOR_MANUAL_UPLOAD".equals(record.status) && record.xesPath != null) {
                    ids.add(id);
                }
            }
        });
        return ids;
    }

    public long resetEpoch() {
        return resetEpoch.get();
    }

    public void resetAll(Path xesDir) {
        resetEpoch.incrementAndGet();
        vacancies.clear();
        startedHiringVacancies.clear();
        synchronized (recentApplications) {
            recentApplications.clear();
        }
        synchronized (scaleUpAlerts) {
            scaleUpAlerts.clear();
        }
        totalApplications.set(0);
        if (xesDir != null && Files.isDirectory(xesDir)) {
            try (var stream = Files.list(xesDir)) {
                stream.filter(p -> p.toString().endsWith(".xes")).forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException ignored) {
                    }
                });
            } catch (IOException ignored) {
            }
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

    public List<Map<String, Object>> snapshotVacancies() {
        List<Map<String, Object>> list = new ArrayList<>();
        vacancies.values().stream()
                .sorted((a, b) -> b.updatedAt.compareTo(a.updatedAt))
                .forEach(record -> {
                    synchronized (record) {
                        Map<String, Object> m = new LinkedHashMap<>();
                        m.put("vacancyId", record.vacancyId);
                        m.put("status", record.status);
                        m.put("applicants", List.copyOf(record.applicants));
                        m.put("applicantProfiles", List.copyOf(record.applicantProfiles));
                        m.put("eventCount", record.events.size());
                        m.put("events", record.events.stream().map(e -> Map.of(
                                "eventName", e.eventName,
                                "role", e.role,
                                "timestamp", Instant.ofEpochMilli(e.timestampEpochMs).toString(),
                                "terminal", e.terminal
                        )).toList());
                        m.put("xesPath", record.xesPath);
                        m.put("xesXml", record.xesXml);
                        m.put("conformanceResult", record.conformanceResult);
                        m.put("errorMessage", record.errorMessage);
                        m.put("updatedAt", record.updatedAt);
                        list.add(m);
                    }
                });
        return list;
    }

    public int totalApplications() {
        return totalApplications.get();
    }

    public List<Map<String, Object>> recentApplications() {
        synchronized (recentApplications) {
            return List.copyOf(recentApplications);
        }
    }

    public List<Map<String, Object>> scaleUpAlerts() {
        synchronized (scaleUpAlerts) {
            return List.copyOf(scaleUpAlerts);
        }
    }

    private static final class VacancyRecord {
        final String vacancyId;
        final List<String> applicants = new ArrayList<>();
        final List<Map<String, Object>> applicantProfiles = new ArrayList<>();
        List<HiringTraceEvent> events = new ArrayList<>();
        String status = "OPEN";
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
