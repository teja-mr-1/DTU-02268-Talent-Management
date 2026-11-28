package dk.dtu.applications.flink;

import dk.dtu.applications.model.Application;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DuplicateApplications extends KeyedProcessFunction<Tuple2<String, String>, Application, Application> {
    private static final Logger log = LoggerFactory.getLogger(DuplicateApplications.class);
    private transient ValueState<Long> submissions;

    public static Tuple2<String, String> extractNameAndPositionKey(Application application) {
        return Tuple2.of(application.name, application.position);
    }

    @Override
    public void open(OpenContext context) {
        submissions = getRuntimeContext().getState(new ValueStateDescriptor<>("submission-count", Long.class));
    }

    @Override
    public void processElement(Application application, Context context, Collector<Application> out) throws Exception {
        Long previous = submissions.value();
        long count = previous == null ? 1 : previous + 1;
        submissions.update(count);

        if (count >= 3) {
            log.warn("Application ignored as suspected spam applicationId={} position={} submissions={}",
                application.applicationId, application.position, count);
            return;
        }
        if (count == 2) {
            log.warn("Possible duplicate application applicationId={} position={}", application.applicationId, application.position);
        }
        out.collect(application);
    }
}
