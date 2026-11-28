package dk.dtu.applications.model;

import jakarta.validation.constraints.NotBlank;

public class Distribution {
    public String simulationId;
    public Assignment[] assignments;

    public static class Assignment {
        @NotBlank public String applicationId;
        @NotBlank public String reviewer;
    }
}
