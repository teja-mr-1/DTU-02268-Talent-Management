package dk.dtu.applications;

import dk.dtu.applications.config.Settings;
import dk.dtu.applications.flink.ApplicationSource;
import dk.dtu.applications.flink.ScaleUpCompletionHandler;
import dk.dtu.applications.model.Application;
import dk.dtu.applications.model.EventType;
import dk.dtu.applications.model.ScaleUpCompletion;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.api.connector.sink2.Sink;
import dk.dtu.applications.model.Command;
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
    public static StreamExecutionEnvironment create(String inboxId, Sink<Command> sink) {
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

        execution.fromSource(new ApplicationSource<>(inboxId, EventType.APPLICATION, Application.class), WatermarkStrategy.noWatermarks(), "Application inbox")
            .uid("applications-source")
            .keyBy(value -> "all-applications")
            .process(new RollingHour()).name("Rolling hour threshold").uid("rolling-hour")
            .sinkTo(sink).name("Start Camunda processes").uid("camunda-start");

        execution.fromSource(new ApplicationSource<>(inboxId, EventType.SCALE_UP_COMPLETED, ScaleUpCompletion.class),
                WatermarkStrategy.noWatermarks(), "Scale-up completion inbox")
            .uid("scale-up-completions-source")
            .keyBy(completion -> "all-completions")
            .process(new ScaleUpCompletionHandler()).name("Record scale-up completion").uid("scale-up-completions")
            .sinkTo(new DiscardingSink<>()).name("Completion handling finished").uid("completions-finished");
        return execution;
    }

}
