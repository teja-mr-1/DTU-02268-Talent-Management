package dk.dtu.applications.flink;

import dk.dtu.applications.model.ScaleUpCompletion;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import static org.junit.jupiter.api.Assertions.*;

class ScaleUpCompletionHandlerTest {
    @Test void recordsCompletionOnceWithoutEmittingDownstreamCommands() throws Exception {
        try (var harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new ScaleUpCompletionHandler()),
                (ScaleUpCompletion completion) -> "all-completions", Types.STRING)) {
            harness.open();
            var completion = new ScaleUpCompletion();
            completion.processInstanceKey = "process-1";
            completion.applicationId = "application-1";
            completion.triggerId = "application-1:scale";
            completion.receivedAt = 100;
            harness.processElement(new StreamRecord<>(completion));
            completion.receivedAt = 200;
            harness.processElement(new StreamRecord<>(completion));
            assertTrue(harness.extractOutputStreamRecords().isEmpty());
            var descriptor = new MapStateDescriptor<>("completed-scale-ups", String.class, Long.class);
            descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofDays(1)).build());
            var completed = harness.getOperator().getKeyedStateBackend().getPartitionedState(
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor);
            assertEquals(100L, completed.get("process-1"));
        }
    }
}
