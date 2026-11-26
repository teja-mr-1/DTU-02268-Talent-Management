package dk.dtu.applications.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dk.dtu.applications.model.Application;
import dk.dtu.applications.model.IntakeEvent;
import dk.dtu.applications.model.EventType;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import java.util.Map;
import java.util.UUID;
import dk.dtu.applications.flink.ApplicationInbox;

@RestController
final class ApplicationController {
    private static final Logger log = LoggerFactory.getLogger(ApplicationController.class);
    private final ApplicationInbox queue;

    ApplicationController(ApplicationInbox queue) { this.queue = queue; }

    @PostMapping("/api/applications")
    ResponseEntity<Map<String, String>> submit(@Valid @RequestBody Submission submission) {
        Application application = new Application();
        application.applicationId = UUID.randomUUID().toString();
        application.receivedAt = System.currentTimeMillis();
        application.name = submission.name();
        application.position = submission.position();
        queue.offer(new IntakeEvent<>(EventType.APPLICATION, application));
        log.info("Application accepted applicationId={} position={}", application.applicationId, application.position);
        return ResponseEntity.accepted().body(Map.of("applicationId", application.applicationId));
    }

    @ExceptionHandler({MethodArgumentNotValidException.class, HttpMessageNotReadableException.class})
    ResponseEntity<Map<String, String>> invalidSubmission() {
        log.debug("Submission rejected: invalid request body");
        return ResponseEntity.badRequest().body(Map.of("error", "Provide a name and a supported position"));
    }

    record Submission(
        @NotBlank @Size(max = 160) String name,
        @NotBlank @Pattern(regexp = "PhD student|Associate professor|Assistant professor") String position) {
        Submission { if (name != null) name = name.strip(); }
    }
}
