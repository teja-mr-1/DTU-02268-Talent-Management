package dk.dtu.applications.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ConformanceResult(
        String modelName,
        String logName,
        int conforming,
        int partiallyViolating,
        int violating,
        int totalStructuralViolations,
        int totalTimeViolations,
        String details) {

    public boolean conformant() {
        return violating == 0
                && partiallyViolating == 0
                && totalStructuralViolations == 0
                && totalTimeViolations == 0;
    }
}
