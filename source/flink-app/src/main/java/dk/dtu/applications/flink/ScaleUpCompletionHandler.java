package dk.dtu.applications.flink;

import dk.dtu.applications.model.ScaleUpCompletion;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;

public class ScaleUpCompletionHandler extends KeyedProcessFunction<String, ScaleUpCompletion, Void> {
    private static final Logger log = LoggerFactory.getLogger(ScaleUpCompletionHandler.class);
    private transient MapState<String, Long> completedScaleUps;

    @Override public void open(OpenContext context) {
        var completed = new MapStateDescriptor<>("completed-scale-ups", String.class, Long.class);
        completed.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofDays(1)).build());
        completedScaleUps = getRuntimeContext().getMapState(completed);
    }
    @Override public void processElement(ScaleUpCompletion completion, Context context, Collector<Void> out) throws Exception {
        completedScaleUps.put(completion.processInstanceKey, completion.receivedAt);
        log.info("Scale-up completion received in Flink processInstanceKey={} applicationId={} triggerId={}",
            completion.processInstanceKey, completion.applicationId, completion.triggerId);
    }
}
