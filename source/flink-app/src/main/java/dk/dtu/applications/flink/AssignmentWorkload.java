package dk.dtu.applications.flink;

import dk.dtu.applications.model.Distribution;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class AssignmentWorkload extends KeyedProcessFunction<String, Distribution, Distribution> {
    private static final Logger log = LoggerFactory.getLogger(AssignmentWorkload.class);
    private static final List<String> EMPLOYEES = List.of("Anna Nielsen", "Lars Petersen", "Maria Larsen");
    private static final long WORKLOAD_GAP_THRESHOLD = 3;
    private transient ListState<String> reviewers;

    @Override public void open(OpenContext context) {
        var state = new ListStateDescriptor<>("weekly-assignment-reviewers", String.class);
        state.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofDays(7))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build());
        reviewers = getRuntimeContext().getListState(state);
    }

    @Override public void processElement(Distribution distribution, Context context, Collector<Distribution> out) throws Exception {
        var totals = new HashMap<String, Long>();
        EMPLOYEES.forEach(employee -> totals.put(employee, 0L));
        for (var assignment : distribution.assignments) reviewers.add(assignment.reviewer);
        for (String reviewer : reviewers.get()) totals.merge(reviewer, 1L, Long::sum);

        long gap = Collections.max(totals.values()) - Collections.min(totals.values());
        if (gap >= WORKLOAD_GAP_THRESHOLD) {
            log.warn("Unequal application assignment workloadGap={} assignmentsByEmployee={}", gap, totals);
        }
        out.collect(distribution);
    }
}
