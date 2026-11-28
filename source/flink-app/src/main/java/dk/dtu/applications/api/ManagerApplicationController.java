package dk.dtu.applications.api;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import dk.dtu.applications.dcr.DcrClient;
import dk.dtu.applications.dcr.ScaleUpProcessState;
import dk.dtu.applications.flink.ApplicationInbox;
import dk.dtu.applications.model.Distribution;
import dk.dtu.applications.model.Distribution.Assignment;
import dk.dtu.applications.model.EventType;
import dk.dtu.applications.model.IntakeEvent;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
final class ManagerApplicationController {
    private static final Logger log = LoggerFactory.getLogger(ManagerApplicationController.class);
    private final DcrClient client;
    private final ScaleUpProcessState state;
    private final ApplicationInbox inbox;

    ManagerApplicationController(DcrClient client,
            ScaleUpProcessState state, ApplicationInbox inbox) {
        this.client = client;
        this.state = state;
        this.inbox = inbox;
    }

    private String requireActiveSimulationId() {
        String id = state.simulationId();
        if (id == null) {
            throw new ResponseStatusException(
                HttpStatus.CONFLICT, "No scale-up process has been started");
        }
        return id;
    }

    @GetMapping(value = "/applications", params = "status=pending")
    List<Object> getPendingApplications() {
        String id = requireActiveSimulationId();
        try {
            client.executeEvent(id, "A1");
        } catch (RuntimeException failure) {
            log.error("DCR manager event failed simulationId={} event=A1", id, failure);
            throw new ResponseStatusException(
                HttpStatus.BAD_GATEWAY, "DCR event could not be executed", failure);
        }
        return List.of();
    }

    @PutMapping("/applications/distribute")
    ResponseEntity<Void> distributeApplications(@Valid @RequestBody DistributionRequest request) {
        var distribution = new Distribution();
        distribution.simulationId = requireActiveSimulationId();
        distribution.assignments = request.assignments().toArray(Assignment[]::new);
        inbox.offer(new IntakeEvent<>(EventType.DISTRIBUTION, distribution));
        return ResponseEntity.accepted().build();
    }

    record DistributionRequest(@NotEmpty List<@NotNull @Valid Assignment> assignments) {}
}
