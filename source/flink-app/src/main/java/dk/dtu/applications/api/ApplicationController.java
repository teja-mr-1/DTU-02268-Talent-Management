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
import dk.dtu.applications.flink.VacancyTraceStore;
import org.springframework.http.HttpStatus;

@RestController
final class ApplicationController {
    private static final Logger log = LoggerFactory.getLogger(ApplicationController.class);
    private final ApplicationInbox queue;
    private final VacancyTraceStore store;

    ApplicationController(ApplicationInbox queue, VacancyTraceStore store) {
        this.queue = queue;
        this.store = store;
    }

    @PostMapping("/api/applications")
    ResponseEntity<Map<String, String>> submit(@Valid @RequestBody Submission submission) {
        String vacancyId = (submission.vacancyId() == null || submission.vacancyId().isBlank())
                ? "VAC-FP-2026-01"
                : submission.vacancyId().strip();
        if (store.isVacancyClosed(vacancyId)) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
                    "error", "Vacancy " + vacancyId + " is already filled and closed. Please enter a new Vacancy ID."));
        }
        Application application = new Application();
        application.applicationId = UUID.randomUUID().toString();
        application.vacancyId = vacancyId;
        application.receivedAt = System.currentTimeMillis();
        application.name = submission.name();
        application.position = submission.position();
        application.email = (submission.email() == null || submission.email().isBlank())
                ? "applicant@dtu-demo.dk"
                : submission.email().strip();
        application.citizenship = (submission.citizenship() == null || submission.citizenship().isBlank())
                ? "Denmark (EU/EEA)"
                : submission.citizenship().strip();
        application.previouslyAtDtu = (submission.previouslyAtDtu() == null || submission.previouslyAtDtu().isBlank())
                ? "No"
                : submission.previouslyAtDtu().strip();
        queue.offer(new IntakeEvent<>(EventType.APPLICATION, application));
        log.info("Application accepted applicationId={} vacancyId={} position={}",
                application.applicationId, application.vacancyId, application.position);
        return ResponseEntity.accepted().body(Map.of(
                "applicationId", application.applicationId,
                "vacancyId", application.vacancyId));
    }

    @ExceptionHandler({MethodArgumentNotValidException.class, HttpMessageNotReadableException.class})
    ResponseEntity<Map<String, String>> invalidSubmission() {
        log.debug("Submission rejected: invalid request body");
        return ResponseEntity.badRequest().body(Map.of("error", "Provide a name and a supported position"));
    }

    record Submission(
        @NotBlank @Size(max = 160) String name,
        @NotBlank @Pattern(regexp = "Full professor|Associate professor|Assistant professor|PhD student") String position,
        @Size(max = 80) String vacancyId,
        @Size(max = 160) String email,
        @Size(max = 100) String citizenship,
        @Size(max = 20) String previouslyAtDtu) {
        Submission { if (name != null) name = name.strip(); }
    }
}
