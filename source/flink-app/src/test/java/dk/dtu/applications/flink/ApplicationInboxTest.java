package dk.dtu.applications.flink;

import dk.dtu.applications.model.Application;
import dk.dtu.applications.model.EventType;
import dk.dtu.applications.model.IntakeEvent;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ApplicationInboxTest {
    @Test void queueSignalsAvailabilityAndRearmsAfterDraining() {
        var inbox = new ApplicationInbox();
        var applicationsAvailable = inbox.available(EventType.APPLICATION);
        var application = new Application();
        inbox.offer(new IntakeEvent<>(EventType.APPLICATION, application));
        assertTrue(applicationsAvailable.isDone());
        assertSame(application, inbox.poll(EventType.APPLICATION, Application.class));
        var waitingAgain = inbox.available(EventType.APPLICATION);
        assertFalse(waitingAgain.isDone());
        inbox.offer(new IntakeEvent<>(EventType.APPLICATION, new Application()));
        assertTrue(waitingAgain.isDone());
    }

    @Test void mismatchedPayloadsAreRejectedBeforeQueueing() {
        assertThrows(IllegalArgumentException.class,
            () -> new IntakeEvent<>(EventType.APPLICATION, "invalid payload"));
        var inbox = new ApplicationInbox();
        assertThrows(IllegalArgumentException.class,
            () -> inbox.poll(EventType.APPLICATION, String.class));
    }
}
