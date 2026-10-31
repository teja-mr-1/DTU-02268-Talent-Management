package dk.dtu.applications.flink;

import dk.dtu.applications.model.Application;
import dk.dtu.applications.model.EventType;
import dk.dtu.applications.model.IntakeEvent;
import dk.dtu.applications.model.ScaleUpCompletion;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ApplicationInboxTest {
    @Test void keysHaveIndependentQueuesCapacityAndAvailability() {
        var inbox = new ApplicationInbox(1);
        inbox.startReading(EventType.APPLICATION);
        inbox.startReading(EventType.SCALE_UP_COMPLETED);
        var applicationsAvailable = inbox.available(EventType.APPLICATION);
        var completionsAvailable = inbox.available(EventType.SCALE_UP_COMPLETED);
        var application = new Application();
        var completion = new ScaleUpCompletion();
        assertTrue(inbox.offer(new IntakeEvent<>(EventType.APPLICATION, application)));
        assertTrue(applicationsAvailable.isDone());
        assertFalse(completionsAvailable.isDone());
        assertFalse(inbox.offer(new IntakeEvent<>(EventType.APPLICATION, new Application())));
        assertTrue(inbox.offer(new IntakeEvent<>(EventType.SCALE_UP_COMPLETED, completion)));
        assertSame(completion, inbox.poll(EventType.SCALE_UP_COMPLETED, ScaleUpCompletion.class));
        assertNull(inbox.poll(EventType.SCALE_UP_COMPLETED, ScaleUpCompletion.class));
        assertSame(application, inbox.poll(EventType.APPLICATION, Application.class));
        var waitingAgain = inbox.available(EventType.APPLICATION);
        assertFalse(waitingAgain.isDone());
        assertTrue(inbox.offer(new IntakeEvent<>(EventType.APPLICATION, new Application())));
        assertTrue(waitingAgain.isDone());
    }

    @Test void readinessAndStoppingArePerKey() {
        var inbox = new ApplicationInbox(1);
        assertFalse(inbox.offer(new IntakeEvent<>(EventType.APPLICATION, new Application())));
        inbox.startReading(EventType.APPLICATION);
        assertFalse(inbox.started().isDone());
        assertTrue(inbox.offer(new IntakeEvent<>(EventType.APPLICATION, new Application())));
        assertFalse(inbox.offer(new IntakeEvent<>(EventType.SCALE_UP_COMPLETED, new ScaleUpCompletion())));
        inbox.startReading(EventType.SCALE_UP_COMPLETED);
        assertTrue(inbox.started().isDone());
        inbox.stopReading(EventType.APPLICATION);
        assertFalse(inbox.offer(new IntakeEvent<>(EventType.APPLICATION, new Application())));
        assertTrue(inbox.offer(new IntakeEvent<>(EventType.SCALE_UP_COMPLETED, new ScaleUpCompletion())));
        inbox.poll(EventType.SCALE_UP_COMPLETED, ScaleUpCompletion.class);
        inbox.stopReading();
        assertFalse(inbox.offer(new IntakeEvent<>(EventType.SCALE_UP_COMPLETED, new ScaleUpCompletion())));
    }

    @Test void mismatchedPayloadsAreRejectedBeforeQueueing() {
        assertThrows(IllegalArgumentException.class,
            () -> new IntakeEvent<>(EventType.APPLICATION, new ScaleUpCompletion()));
        var inbox = new ApplicationInbox(1);
        assertThrows(IllegalArgumentException.class,
            () -> inbox.poll(EventType.APPLICATION, ScaleUpCompletion.class));
    }
}
