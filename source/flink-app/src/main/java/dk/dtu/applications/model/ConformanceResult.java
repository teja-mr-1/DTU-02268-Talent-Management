package dk.dtu.applications.model;

import java.time.Instant;

/**
 * Structured summary of a Process Doctor DCR conformance check run.
 */
public record ConformanceResult(
        String modelName,
        String logName,
        int conforming,
        int partiallyViolating,
        int violating,
        int totalStructuralViolations,
        boolean conformant,
        String details,
        Instant checkedAt) {

    public static ConformanceResult of(
            String modelName,
            String logName,
            int conforming,
            int partiallyViolating,
            int violating,
            int totalStructuralViolations,
            String details) {
        boolean isConformant = violating == 0
                && partiallyViolating == 0
                && totalStructuralViolations == 0;
        return new ConformanceResult(
                modelName,
                logName,
                conforming,
                partiallyViolating,
                violating,
                totalStructuralViolations,
                isConformant,
                details,
                Instant.now());
    }
}
