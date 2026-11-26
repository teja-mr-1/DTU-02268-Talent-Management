package dk.dtu.applications.api;

import dk.dtu.applications.flink.ApplicationInbox;
import dk.dtu.applications.flink.VacancyTraceStore;
import dk.dtu.applications.model.ConformanceChecker;
import dk.dtu.applications.model.ConformanceResult;
import dk.dtu.applications.model.EventType;
import dk.dtu.applications.model.HiringTraceEvent;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TraceControllerTest {

    @Test
    void enqueuesSingleEventFromRestConnectorAndAppliesDefaults() {
        ApplicationInbox inbox = new ApplicationInbox();
        VacancyTraceStore store = new VacancyTraceStore();
        TraceController controller = new TraceController(inbox, store, new ConformanceChecker());

        var response = controller.ingestEvent(new TraceController.TraceEventRequest(
                "VAC-FP-10",
                "Open Full Professor Vacancy",
                null,
                "2026-11-26T12:00:00Z",
                false));

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
    void enqueuesBatchEventsAndMarksLastEventTerminalByDefault() {
        ApplicationInbox inbox = new ApplicationInbox();
        VacancyTraceStore store = new VacancyTraceStore();
        TraceController controller = new TraceController(inbox, store, new ConformanceChecker());

        var response = controller.ingestBatch(new TraceController.TraceBatchRequest(
                "VAC-FP-20",
                List.of(
                        new TraceController.BatchEventItem("Confirm funding availability", "Head of Finance", "2026-11-26T10:00:00Z"),
                        new TraceController.BatchEventItem("Close position unfilled", "HR", "2026-11-26T11:00:00Z")
                ),
                null));

        assertEquals(HttpStatus.ACCEPTED, response.getStatusCode());
        assertEquals(2, response.getBody().get("acceptedEvents"));

        HiringTraceEvent first = inbox.poll(EventType.HIRING_TRACE_EVENT, HiringTraceEvent.class);
        HiringTraceEvent second = inbox.poll(EventType.HIRING_TRACE_EVENT, HiringTraceEvent.class);
        assertNotNull(first);
        assertNotNull(second);
        assertFalse(first.terminal);
        assertTrue(second.terminal);
        assertEquals("HR", second.role);
    }

    @Test
    void rejectsInvalidTimestamp() {
        TraceController controller = new TraceController(
                new ApplicationInbox(), new VacancyTraceStore(), new ConformanceChecker());

        assertThrows(IllegalArgumentException.class, () -> controller.ingestEvent(
                new TraceController.TraceEventRequest("VAC-1", "Task", "HR", "not-a-timestamp", false)));
    }

    @Test
    void servesPublishedXesAndRunsManualConformanceCheck() {
        VacancyTraceStore store = new VacancyTraceStore();
        store.markChecking(
                "VAC-FP-30",
                List.of(new HiringTraceEvent("VAC-FP-30", "Close position unfilled", "HR", 1_700_000_000_000L, true)),
                "/data/xes/VAC-FP-30.xes",
                "<log><trace/></log>");

        ConformanceResult expected = ConformanceResult.of(
                "Full_Professor_Hiring_Compliance.dcr.xml",
                "VAC-FP-30.xes",
                1, 0, 0, 0, "Trace Variant #1");

        ConformanceChecker stubChecker = new ConformanceChecker() {
            @Override
            public ConformanceResult checkXml(String xesXml, String optionalModelXml, String logName) {
                assertEquals("<log><trace/></log>", xesXml);
                assertEquals("VAC-FP-30.xes", logName);
                return expected;
            }
        };

        TraceController controller = new TraceController(new ApplicationInbox(), store, stubChecker);

        var xesResp = controller.downloadXes("VAC-FP-30");
        assertEquals(HttpStatus.OK, xesResp.getStatusCode());
        assertEquals("<log><trace/></log>", xesResp.getBody());

        var checkResp = controller.checkVacancy("VAC-FP-30", null);
        assertEquals(HttpStatus.OK, checkResp.getStatusCode());
        assertEquals(expected, checkResp.getBody());
        assertEquals("CONFORMANT", store.snapshotVacancy("VAC-FP-30").orElseThrow().get("status"));

        assertEquals(HttpStatus.NOT_FOUND, controller.getTrace("UNKNOWN").getStatusCode());
        assertEquals(HttpStatus.NOT_FOUND, controller.downloadXes("UNKNOWN").getStatusCode());
        assertEquals(HttpStatus.NOT_FOUND, controller.checkVacancy("UNKNOWN", null).getStatusCode());
    }
}
