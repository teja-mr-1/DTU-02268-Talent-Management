package dk.dtu.applications.flink;

import dk.dtu.applications.ApplicationJob;
import dk.dtu.applications.camunda.ApplicationReceivedSink;
import dk.dtu.applications.camunda.ScaleUpRequestedSink;
import dk.dtu.applications.config.Settings;
import dk.dtu.applications.model.ApplicationReceived;
import dk.dtu.applications.model.ConformanceChecker;
import dk.dtu.applications.model.ScaleUpRequested;
import jakarta.annotation.PreDestroy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
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
    private final VacancyTraceStore traceStore;
    private final ConformanceChecker conformanceChecker;
    private String inboxId;
    private String runtimeId;
    private StreamExecutionEnvironment execution;
    private JobClient job;
    private volatile boolean stopping;

    EmbeddedFlink(
            ApplicationInbox inbox,
            ApplicationContext context,
            ApplicationReceivedSink applicationSink,
            ScaleUpRequestedSink scaleUpSink,
            VacancyTraceStore traceStore,
            ConformanceChecker conformanceChecker) {
        this.inbox = inbox;
        this.context = context;
        this.applicationSink = applicationSink;
        this.scaleUpSink = scaleUpSink;
        this.traceStore = traceStore;
        this.conformanceChecker = conformanceChecker;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        boolean localSim = Settings.localCamundaSimulator();
        log.info("Starting embedded Flink job (camundaMode={})", localSim ? "local-simulator" : "camunda-8");
        inboxId = InboxRegistry.register(inbox);
        runtimeId = RuntimeRegistry.register(traceStore, conformanceChecker);

        Sink<ApplicationReceived> effectiveAppSink = localSim
                ? new LocalApplicationSink(runtimeId)
                : applicationSink.withRuntimeId(runtimeId);
        Sink<ScaleUpRequested> effectiveScaleSink = localSim
                ? new LocalScaleUpSink(runtimeId)
                : scaleUpSink.withRuntimeId(runtimeId);

        execution = ApplicationJob.create(inboxId, runtimeId, effectiveAppSink, effectiveScaleSink);
        job = execution.executeAsync("Talent hiring full pipeline");
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

    @PreDestroy
    void stop() throws Exception {
        stopping = true;
        log.info("Stopping embedded Flink");
        try {
            if (job != null) {
                job.cancel().get(20, TimeUnit.SECONDS);
            }
        } finally {
            try {
                if (execution != null) {
                    execution.close();
                }
            } finally {
                if (inboxId != null) {
                    InboxRegistry.remove(inboxId);
                }
                if (runtimeId != null) {
                    RuntimeRegistry.remove(runtimeId);
                }
                log.info("Embedded Flink resources released");
            }
        }
    }

    private static final class LocalApplicationSink implements Sink<ApplicationReceived> {
        private final String runtimeId;

        LocalApplicationSink(String runtimeId) {
            this.runtimeId = runtimeId;
        }

        @Override
        public SinkWriter<ApplicationReceived> createWriter(WriterInitContext context) {
            return new SinkWriter<>() {
                @Override
                public void write(ApplicationReceived event, Context ctx) {
                    RuntimeRegistry.Services services = RuntimeRegistry.get(runtimeId);
                    if (services != null && services.store() != null) {
                        services.store().recordApplication(event.application, event.count);
                    }
                    log.info("[Local Camunda Simulator] Started hiring process applicationId={} vacancyId={} rollingHourCount={}",
                            event.application.applicationId, event.application.vacancyId, event.count);
                }

                @Override public void flush(boolean endOfInput) {}
                @Override public void close() {}
            };
        }
    }

    private static final class LocalScaleUpSink implements Sink<ScaleUpRequested> {
        private final String runtimeId;

        LocalScaleUpSink(String runtimeId) {
            this.runtimeId = runtimeId;
        }

        @Override
        public SinkWriter<ScaleUpRequested> createWriter(WriterInitContext context) {
            return new SinkWriter<>() {
                @Override
                public void write(ScaleUpRequested event, Context ctx) {
                    RuntimeRegistry.Services services = RuntimeRegistry.get(runtimeId);
                    if (services != null && services.store() != null) {
                        services.store().recordScaleUpAlert(event.application, event.count);
                    }
                    log.warn("[Local Camunda Simulator] Scale-up alert triggered! vacancyId={} applicationsInLastHour={}",
                            event.application.vacancyId, event.count);
                }

                @Override public void flush(boolean endOfInput) {}
                @Override public void close() {}
            };
        }
    }
}
