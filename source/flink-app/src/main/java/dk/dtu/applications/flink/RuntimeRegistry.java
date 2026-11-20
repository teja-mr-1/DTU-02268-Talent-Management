package dk.dtu.applications.flink;

import dk.dtu.applications.model.ConformanceChecker;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Static runtime registry so Flink operators/sinks can access non-serializable Spring services.
 */
public final class RuntimeRegistry {
    private static final ConcurrentHashMap<String, Services> REGISTRY = new ConcurrentHashMap<>();

    private RuntimeRegistry() {
    }

    public static String register(VacancyTraceStore store, ConformanceChecker checker) {
        String id = UUID.randomUUID().toString();
        REGISTRY.put(id, new Services(store, checker));
        return id;
    }

    public static Services get(String id) {
        return id == null ? null : REGISTRY.get(id);
    }

    public static void remove(String id) {
        if (id != null) {
            REGISTRY.remove(id);
        }
    }

    public record Services(VacancyTraceStore store, ConformanceChecker checker) {
    }
}
