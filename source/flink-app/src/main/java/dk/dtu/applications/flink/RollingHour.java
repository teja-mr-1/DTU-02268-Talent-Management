package dk.dtu.applications.flink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dk.dtu.applications.model.Application;
import dk.dtu.applications.model.ApplicationReceived;
import dk.dtu.applications.model.ScaleUpRequested;
import org.apache.flink.util.OutputTag;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.util.ArrayList;
import java.util.List;

public class RollingHour extends KeyedProcessFunction<String, Application, ApplicationReceived> {
    private static final Logger log = LoggerFactory.getLogger(RollingHour.class);
    public static final OutputTag<ScaleUpRequested> SCALE_UP = new OutputTag<>("scale-up", org.apache.flink.api.common.typeinfo.TypeInformation.of(ScaleUpRequested.class));
    static final long HOUR = 3_600_000L;
    private final String runtimeId;
    private transient ListState<Long> arrivals;
    private transient ValueState<Long> lastSeenEpoch;

    public RollingHour() {
        this(null);
    }

    public RollingHour(String runtimeId) {
        this.runtimeId = runtimeId;
    }

    @Override public void open(OpenContext context) {
        arrivals = getRuntimeContext().getListState(new ListStateDescriptor<>("arrival-times", Long.class));
        lastSeenEpoch = getRuntimeContext().getState(new ValueStateDescriptor<>("rolling-hour-epoch", Long.class));
    }
    @Override public void processElement(Application application, Context context, Collector<ApplicationReceived> out) throws Exception {
        if (runtimeId != null) {
            RuntimeRegistry.Services services = RuntimeRegistry.get(runtimeId);
            long currentEpoch = (services != null && services.store() != null) ? services.store().resetEpoch() : 0L;
            Long seenEpoch = lastSeenEpoch.value();
            if (seenEpoch == null || seenEpoch.longValue() != currentEpoch) {
                arrivals.clear();
                lastSeenEpoch.update(currentEpoch);
            }
        }
        long now = context.timerService().currentProcessingTime();
        List<Long> active = getLastHourApplications(now);
        boolean wasAbove = active.size() > 5;
        active.add(now);
        arrivals.update(active);
        log.info("Application counted applicationId={} applicationsInLastHour={}", application.applicationId, active.size());
        out.collect(new ApplicationReceived(application, active.size()));
        if (!wasAbove && active.size() > 5) {
            log.info("Scale-up threshold crossed applicationId={} applicationsInLastHour={}", application.applicationId, active.size());
            context.output(SCALE_UP, new ScaleUpRequested(application, active.size()));
        }
    }
    private List<Long> getLastHourApplications(long now) throws Exception {
        List<Long> active = new ArrayList<>();
        for (Long timestamp : arrivals.get()) if (timestamp > now - HOUR) active.add(timestamp);
        return active;
    }
}
