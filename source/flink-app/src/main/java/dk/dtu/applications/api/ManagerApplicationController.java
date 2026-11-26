package dk.dtu.applications.api;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import dk.dtu.applications.dcr.DcrClient;
import dk.dtu.applications.dcr.ScaleUpProcessState;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
final class ManagerApplicationController {
    private static final Logger log = LoggerFactory.getLogger(ManagerApplicationController.class);
    private final DcrClient client;
    private final ScaleUpProcessState state;

    ManagerApplicationController(DcrClient client,
            ScaleUpProcessState state) {
        this.client = client;
        this.state = state;
    }

    private void execute(String event) {
        String id = state.simulationId();
        if (id == null) {
            log.warn("Cannot execute DCR event: no scale-up simulation event={}", event);
            throw new ResponseStatusException(
                HttpStatus.CONFLICT, "No scale-up process has been started");
        }
        try {
            client.executeEvent(id, event);
        } catch (RuntimeException failure) {
            log.error("DCR manager event failed simulationId={} event={}", id, event, failure);
            throw new ResponseStatusException(
                HttpStatus.BAD_GATEWAY, "DCR event could not be executed", failure);
        }
    }
    @GetMapping(value = "/applications", params = "status=pending")
    List<Object> getPendingApplications() {
        execute("A1");
        return List.of();
    }

    @PutMapping("/applications/distribute")
    ResponseEntity<Void> distributeApplications() {
        execute("A12");
        return ResponseEntity.noContent().build();
    }
}
