package dk.dtu.applications.api;

import dk.dtu.applications.flink.ApplicationInbox;
import dk.dtu.applications.flink.VacancyTraceStore;
import dk.dtu.applications.model.EventType;
import dk.dtu.applications.model.HiringTraceEvent;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TraceControllerTest {

    @Test
    void enqueuesTraceEventFromRestConnectorAndAppliesDefaults() {
        ApplicationInbox inbox = new ApplicationInbox();
        VacancyTraceStore store = new VacancyTraceStore();
        TraceController controller = new TraceController(inbox, store);

        var response = controller.ingestEvent(new TraceController.TraceEventRequest(
                "VAC-FP-10", "Open Full Professor Vacancy", null, "2026-11-26T12:00:00Z", false));

        assertEquals(HttpStatus.ACCEPTED, response.getStatusCode());
        HiringTraceEvent queued = inbox.poll(EventType.HIRING_TRACE_EVENT, HiringTraceEvent.class);
        assertNotNull(queued);
        assertEquals("VAC-FP-10", queued.vacancyId);
        assertEquals("Open Full Professor Vacancy", queued.eventName);
        assertEquals("System", queued.role);
        assertEquals(Instant.parse("2026-11-26T12:00:00Z").toEpochMilli(), queued.timestampEpochMs);
        assertFalse(queued.terminal);
    }

    @Test
    void rejectsInvalidTimestamp() {
        TraceController controller = new TraceController(new ApplicationInbox(), new VacancyTraceStore());
        assertThrows(IllegalArgumentException.class, () -> controller.ingestEvent(
                new TraceController.TraceEventRequest("VAC-1", "Task", "HR", "not-a-timestamp", false)));
    }

    @Test
    void returnsSavedTracesAndXesXml() {
        VacancyTraceStore store = new VacancyTraceStore();
        store.markChecking(
                "VAC-FP-30",
                List.of(new HiringTraceEvent("VAC-FP-30", "Close position unfilled", "HR", 1_700_000_000_000L, true)),
                "/data/xes/VAC-FP-30.xes",
                "<log><trace/></log>");

        TraceController controller = new TraceController(new ApplicationInbox(), store);

        assertEquals(1, controller.listTraces().size());
        assertEquals(HttpStatus.OK, controller.getXes("VAC-FP-30").getStatusCode());
        assertEquals("<log><trace/></log>", controller.getXes("VAC-FP-30").getBody());
        assertEquals(HttpStatus.NOT_FOUND, controller.getXes("UNKNOWN").getStatusCode());
    }
}
