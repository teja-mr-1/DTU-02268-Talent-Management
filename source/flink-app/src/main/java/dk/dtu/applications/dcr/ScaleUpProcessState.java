package dk.dtu.applications.dcr;

import org.springframework.stereotype.Component;

@Component
public final class ScaleUpProcessState {
    private volatile String simulationId;

    public void setSimulationId(String id) { simulationId = id; }
    public String simulationId() { return simulationId; }
}
