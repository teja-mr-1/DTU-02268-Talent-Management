package dk.dtu.applications.model;

import java.time.Instant;

/**
 * Structured summary of a DCR conformance check run powered by Process Doctor's dcr-engine.
 */
public record ConformanceResult(
        String modelName,
        String logName,
        int conforming,
        int partiallyViolating,
        int violating,
        int totalStructuralViolations,
        int totalTimeViolations,
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
            int totalTimeViolations,
            String details) {
        boolean isConformant = violating == 0
                && partiallyViolating == 0
                && totalStructuralViolations == 0
                && totalTimeViolations == 0;
        return new ConformanceResult(
                modelName,
                logName,
                conforming,
                partiallyViolating,
                violating,
                totalStructuralViolations,
                totalTimeViolations,
                isConformant,
                details,
                Instant.now());
    }
}
