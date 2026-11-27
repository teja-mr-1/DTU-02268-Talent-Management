package dk.dtu.applications;

import dk.dtu.applications.config.Settings;
import dk.dtu.applications.dcr.ScaleUpRequestedSink;
import dk.dtu.applications.flink.ApplicationSource;
import dk.dtu.applications.flink.ConformanceCheckSink;
import dk.dtu.applications.flink.RollingHour;
import dk.dtu.applications.flink.VacancyTraceCollector;
import dk.dtu.applications.model.Application;
import dk.dtu.applications.model.ApplicationReceived;
import dk.dtu.applications.model.EventType;
import dk.dtu.applications.model.HiringTraceEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.file.Path;
import java.time.Duration;

public class ApplicationJob {
    public static StreamExecutionEnvironment create(String inboxId, Sink<ApplicationReceived> applicationSink, ScaleUpRequestedSink scaleUpSink) {
        return create(inboxId, null, applicationSink, scaleUpSink);
    }

    public static StreamExecutionEnvironment create(String inboxId, String runtimeId, Sink<ApplicationReceived> applicationSink, ScaleUpRequestedSink scaleUpSink) {
        Configuration config = new Configuration();
        config.set(CoreOptions.DEFAULT_PARALLELISM, 1);
        config.set(CoreOptions.CLASSLOADER_RESOLVE_ORDER, "parent-first");
        config.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(10));
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY,
            Settings.value("CHECKPOINT_DIRECTORY", Path.of("data/checkpoints").toAbsolutePath().toUri().toString()));
        config.set(CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
            ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 100);
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(10));
        StreamExecutionEnvironment execution = StreamExecutionEnvironment.createLocalEnvironment(config);

        var outcomes = execution.fromSource(new ApplicationSource<>(inboxId, EventType.APPLICATION, Application.class), WatermarkStrategy.noWatermarks(), "Application inbox")
            .uid("applications-source")
            .keyBy(value -> "all-applications")
            .process(new RollingHour(inboxId)).name("Rolling hour threshold").uid("rolling-hour");
        outcomes.sinkTo(applicationSink).name("Start application process").uid("camunda-start");
        outcomes.getSideOutput(RollingHour.SCALE_UP)
            .sinkTo(scaleUpSink.forInbox(inboxId)).name("Start scale-up process").uid("camunda-scale-up");

        String xesDir = Settings.value("XES_OUTPUT_DIR", Path.of("data/xes").toAbsolutePath().toString());
        execution.fromSource(new ApplicationSource<>(inboxId, EventType.HIRING_TRACE_EVENT, HiringTraceEvent.class),
                WatermarkStrategy.noWatermarks(), "Hiring trace event inbox")
            .uid("hiring-traces-source")
            .keyBy(event -> event.vacancyId)
            .process(new VacancyTraceCollector(runtimeId, xesDir))
            .name("Collect per-vacancy XES trace").uid("vacancy-trace-collector")
            .sinkTo(new ConformanceCheckSink(runtimeId))
            .name("Process Doctor DCR conformance check").uid("process-doctor-conformance");
        return execution;
    }
}
