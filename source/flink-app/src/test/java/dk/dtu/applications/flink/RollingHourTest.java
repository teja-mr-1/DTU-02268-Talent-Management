package dk.dtu.applications.flink;

import dk.dtu.applications.model.Application;

import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RollingHourTest {
    @Test void sixthTriggersOnceAndExpiryRearms() throws Exception {
        try (var harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new RollingHour()),
                (Application application) -> "all", Types.STRING)) {
            harness.open();
            harness.setProcessingTime(100);
            for (int i = 0; i < 5; i++) harness.processElement(new StreamRecord<>(new Application()));
            assertEquals(5, harness.extractOutputStreamRecords().size());
            assertTrue(harness.extractOutputStreamRecords().stream().noneMatch(record -> record.getValue().scaleUp));
            harness.getOutput().clear();
            harness.processElement(new StreamRecord<>(new Application()));
            assertEquals(2, harness.extractOutputStreamRecords().size());
            assertTrue(harness.extractOutputStreamRecords().get(1).getValue().scaleUp);
            harness.getOutput().clear();
            harness.processElement(new StreamRecord<>(new Application()));
            assertEquals(1, harness.extractOutputStreamRecords().size());
            harness.getOutput().clear();
            harness.setProcessingTime(100 + RollingHour.HOUR);
            for (int i = 0; i < 6; i++) harness.processElement(new StreamRecord<>(new Application()));
            assertEquals(7, harness.extractOutputStreamRecords().size());
            assertEquals(1, harness.extractOutputStreamRecords().stream().filter(record -> record.getValue().scaleUp).count());
        }
    }

    @Test void rollingBoundaryExpiresOnlyOlderApplications() throws Exception {
        try (var harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new RollingHour()),
                (Application application) -> "all", Types.STRING)) {
            harness.open();
            harness.setProcessingTime(100);
            harness.processElement(new StreamRecord<>(new Application()));
            harness.setProcessingTime(200);
            for (int i = 0; i < 5; i++) harness.processElement(new StreamRecord<>(new Application()));
            harness.getOutput().clear();
            // At exactly one hour, only the first arrival expires: count goes 6 -> 5.
            harness.setProcessingTime(100 + RollingHour.HOUR);
            harness.processElement(new StreamRecord<>(new Application()));
            assertEquals(2, harness.extractOutputStreamRecords().size());
            assertEquals(6, harness.extractOutputStreamRecords().get(0).getValue().count);
            assertTrue(harness.extractOutputStreamRecords().get(1).getValue().scaleUp);
        }
    }

}
