package dk.dtu.applications;

import dk.dtu.applications.dcr.ScaleUpRequestedSink;
import dk.dtu.applications.dcr.DistributionSink;
import dk.dtu.applications.config.Settings;
import dk.dtu.applications.flink.ApplicationSource;
import dk.dtu.applications.flink.AssignmentWorkload;
import dk.dtu.applications.flink.DuplicateApplications;
import dk.dtu.applications.model.Application;
import dk.dtu.applications.model.Distribution;
import dk.dtu.applications.model.EventType;
import org.apache.flink.api.connector.sink2.Sink;
import dk.dtu.applications.model.ApplicationReceived;
import dk.dtu.applications.flink.RollingHour;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import java.time.Duration;
import java.nio.file.Path;

public class ApplicationJob {
    public static StreamExecutionEnvironment create(String inboxId, Sink<ApplicationReceived> applicationSink,
            ScaleUpRequestedSink scaleUpSink, DistributionSink distributionSink) {
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
            .keyBy(DuplicateApplications::extractNameAndPositionKey)
            .process(new DuplicateApplications()).name("Application duplicate check").uid("application-duplicate-check")
            .keyBy(value -> "all-applications")
            .process(new RollingHour(inboxId)).name("Rolling hour threshold").uid("rolling-hour");
        outcomes.sinkTo(applicationSink).name("Start application process").uid("camunda-start");
        outcomes.getSideOutput(RollingHour.SCALE_UP)
            .sinkTo(scaleUpSink.forInbox(inboxId)).name("Start scale-up process").uid("camunda-scale-up");

        execution.fromSource(new ApplicationSource<>(inboxId, EventType.DISTRIBUTION, Distribution.class),
                WatermarkStrategy.noWatermarks(), "Distribution inbox")
            .uid("distribution-source")
            .keyBy(distribution -> "all-distributions")
            .process(new AssignmentWorkload()).name("Analyze assignment workload").uid("assignment-workload")
            .sinkTo(distributionSink).name("Execute DCR distribution").uid("dcr-distribution");

        return execution;
    }

}
