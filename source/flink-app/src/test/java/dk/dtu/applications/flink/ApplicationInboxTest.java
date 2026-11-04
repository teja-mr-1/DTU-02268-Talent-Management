package dk.dtu.applications.flink;

import dk.dtu.applications.model.Application;
import dk.dtu.applications.model.EventType;
import dk.dtu.applications.model.IntakeEvent;
import dk.dtu.applications.model.ScaleUpCompletion;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ApplicationInboxTest {
    @Test void keysHaveIndependentQueuesAndAvailability() {
        var inbox = new ApplicationInbox();
        var applicationsAvailable = inbox.available(EventType.APPLICATION);
        var completionsAvailable = inbox.available(EventType.SCALE_UP_COMPLETED);
        var application = new Application();
        var completion = new ScaleUpCompletion();
        inbox.offer(new IntakeEvent<>(EventType.APPLICATION, application));
        assertTrue(applicationsAvailable.isDone());
        assertFalse(completionsAvailable.isDone());
        inbox.offer(new IntakeEvent<>(EventType.SCALE_UP_COMPLETED, completion));
        assertSame(completion, inbox.poll(EventType.SCALE_UP_COMPLETED, ScaleUpCompletion.class));
        assertNull(inbox.poll(EventType.SCALE_UP_COMPLETED, ScaleUpCompletion.class));
        assertSame(application, inbox.poll(EventType.APPLICATION, Application.class));
        var waitingAgain = inbox.available(EventType.APPLICATION);
        assertFalse(waitingAgain.isDone());
        inbox.offer(new IntakeEvent<>(EventType.APPLICATION, new Application()));
        assertTrue(waitingAgain.isDone());
    }

    @Test void mismatchedPayloadsAreRejectedBeforeQueueing() {
        assertThrows(IllegalArgumentException.class,
            () -> new IntakeEvent<>(EventType.APPLICATION, new ScaleUpCompletion()));
        var inbox = new ApplicationInbox();
        assertThrows(IllegalArgumentException.class,
            () -> inbox.poll(EventType.APPLICATION, ScaleUpCompletion.class));
    }
}
