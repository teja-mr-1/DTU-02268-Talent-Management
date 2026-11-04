package dk.dtu.applications.flink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dk.dtu.applications.ApplicationJob;
import dk.dtu.applications.camunda.ApplicationReceivedSink;
import dk.dtu.applications.camunda.ScaleUpRequestedSink;
import jakarta.annotation.PreDestroy;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import java.util.concurrent.TimeUnit;

@Component
final class EmbeddedFlink implements ApplicationRunner {
    private static final Logger log = LoggerFactory.getLogger(EmbeddedFlink.class);
    private final ApplicationInbox inbox;
    private final ApplicationContext context;
    private final ApplicationReceivedSink applicationSink;
    private final ScaleUpRequestedSink scaleUpSink;
    private String inboxId;
    private StreamExecutionEnvironment execution;
    private JobClient job;
    private volatile boolean stopping;

    EmbeddedFlink(ApplicationInbox inbox, ApplicationContext context, ApplicationReceivedSink applicationSink, ScaleUpRequestedSink scaleUpSink) {
        this.inbox = inbox;
        this.context = context;
        this.applicationSink = applicationSink;
        this.scaleUpSink = scaleUpSink;
    }

    @Override public void run(ApplicationArguments args) throws Exception {
        log.info("Starting embedded Flink job");
        inboxId = InboxRegistry.register(inbox);
        execution = ApplicationJob.create(inboxId, applicationSink, scaleUpSink);
        job = execution.executeAsync("Talent application intake");
        log.info("Embedded Flink job submitted jobId={}", job.getJobID());
        job.getJobExecutionResult().whenComplete((result, failure) -> {
            if (failure != null && !stopping) {
                log.error("Embedded Flink job failed jobId={}", job.getJobID(), failure);
            } else {
                log.info("Embedded Flink job stopped jobId={}", job.getJobID());
            }
            AvailabilityChangeEvent.publish(context, ReadinessState.REFUSING_TRAFFIC);
        });
    }

    @PreDestroy void stop() throws Exception {
        stopping = true;
        log.info("Stopping embedded Flink");
        try {
            if (job != null) job.cancel().get(20, TimeUnit.SECONDS);
        } finally {
            try { if (execution != null) execution.close(); }
            finally {
                if (inboxId != null) InboxRegistry.remove(inboxId);
                log.info("Embedded Flink resources released");
            }
        }
    }
}
