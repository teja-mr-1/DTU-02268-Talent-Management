package dk.dtu.applications.flink;

import dk.dtu.applications.model.HiringTraceEvent;
import dk.dtu.applications.model.VacancyLogReady;
import dk.dtu.applications.model.XesLogBuilder;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Step 3 Flink operator: collects workflow trace events per vacancyId into keyed
 * Flink state and publishes an IEEE XES trace file once a terminal event arrives.
 */
public class VacancyTraceCollector extends KeyedProcessFunction<String, HiringTraceEvent, VacancyLogReady> {
    private static final Logger log = LoggerFactory.getLogger(VacancyTraceCollector.class);

    private final String runtimeId;
    private final String xesOutputDir;
    private transient ListState<HiringTraceEvent> bufferedEvents;

    public VacancyTraceCollector(String runtimeId, String xesOutputDir) {
        this.runtimeId = runtimeId;
        this.xesOutputDir = xesOutputDir;
    }

    @Override
    public void open(OpenContext context) {
        bufferedEvents = getRuntimeContext().getListState(
                new ListStateDescriptor<>("vacancy-trace-events", HiringTraceEvent.class));
    }

    @Override
    public void processElement(HiringTraceEvent event, Context context, Collector<VacancyLogReady> out) throws Exception {
        List<HiringTraceEvent> events = new ArrayList<>();
        for (HiringTraceEvent existing : bufferedEvents.get()) {
            events.add(existing);
        }
        events.add(event);
        bufferedEvents.update(events);

        log.info("Trace event buffered vacancyId={} eventName='{}' role='{}' terminal={} count={}",
                event.vacancyId, event.eventName, event.role, event.terminal, events.size());

        RuntimeRegistry.Services services = RuntimeRegistry.get(runtimeId);
        if (!event.terminal) {
            if (services != null && services.store() != null) {
                services.store().recordTraceEvent(event.vacancyId, events);
            }
            return;
        }

        XesLogBuilder builder = new XesLogBuilder();
        XesLogBuilder.XesTrace trace = builder.addTrace(event.vacancyId);
        for (HiringTraceEvent item : events) {
            builder.appendEvent(trace, item.eventName, item.role, Instant.ofEpochMilli(item.timestampEpochMs));
        }

        String safeName = event.vacancyId.replaceAll("[^a-zA-Z0-9._-]", "_");
        Path target = Path.of(xesOutputDir, safeName + ".xes").toAbsolutePath();
        builder.publish(target);
        String xml = builder.toXml();

        if (services != null && services.store() != null) {
            services.store().markChecking(event.vacancyId, events, target.toString(), xml);
        }

        bufferedEvents.clear();
        log.info("Published vacancy XES log vacancyId={} path={} events={}", event.vacancyId, target, events.size());
        out.collect(new VacancyLogReady(event.vacancyId, target.toString(), xml, events.size()));
    }
}
