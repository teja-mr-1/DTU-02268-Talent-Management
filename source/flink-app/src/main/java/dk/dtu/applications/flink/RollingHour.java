package dk.dtu.applications.flink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dk.dtu.applications.model.Application;
import dk.dtu.applications.model.Command;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.util.ArrayList;
import java.util.List;

public class RollingHour extends KeyedProcessFunction<String, Application, Command> {
    private static final Logger log = LoggerFactory.getLogger(RollingHour.class);
    static final long HOUR = 3_600_000L;
    private transient ListState<Long> arrivals;
    @Override public void open(OpenContext context) {
        arrivals = getRuntimeContext().getListState(new ListStateDescriptor<>("arrival-times", Long.class));
    }
    @Override public void processElement(Application application, Context context, Collector<Command> out) throws Exception {
        long now = context.timerService().currentProcessingTime();
        List<Long> active = getLastHourApplications(now);
        boolean wasAbove = active.size() > 5;
        active.add(now);
        arrivals.update(active);
        context.timerService().registerProcessingTimeTimer(now + HOUR);
        log.info("Application counted applicationId={} applicationsInLastHour={}", application.applicationId, active.size());
        out.collect(new Command(application, false, active.size()));
        if (!wasAbove && active.size() > 5) {
            log.info("Scale-up threshold crossed applicationId={} applicationsInLastHour={}", application.applicationId, active.size());
            out.collect(new Command(application, true, active.size()));
        }
    }
    @Override public void onTimer(long time, OnTimerContext context, Collector<Command> out) throws Exception {
        List<Long> active = getLastHourApplications(context.timerService().currentProcessingTime());
        log.debug("Rolling-hour cleanup timer={} applicationsInLastHour={}", time, active.size());
        if (active.isEmpty()) arrivals.clear();
        else arrivals.update(active);
    }
    private List<Long> getLastHourApplications(long now) throws Exception {
        List<Long> active = new ArrayList<>();
        for (Long timestamp : arrivals.get()) if (timestamp > now - HOUR) active.add(timestamp);
        return active;
    }
}
