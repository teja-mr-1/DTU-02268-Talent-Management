package dk.dtu.applications.flink;

import dk.dtu.applications.model.HiringTraceEvent;
import dk.dtu.applications.model.VacancyLogReady;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class VacancyTraceCollectorTest {

    @Test
    void collectsPerVacancyEventsAndPublishesXesOnTerminalEvent(@TempDir Path tempDir) throws Exception {
        VacancyTraceStore store = new VacancyTraceStore(false);
        String runtimeId = RuntimeRegistry.register(store, null);
        try (var harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new VacancyTraceCollector(runtimeId, tempDir.toString())),
                (HiringTraceEvent event) -> event.vacancyId,
                Types.STRING)) {
            harness.open();

            harness.processElement(new StreamRecord<>(
                    new HiringTraceEvent("VAC-FP-1", "Confirm funding availability", "Head of Finance", 1_700_000_000_000L, false)));
            harness.processElement(new StreamRecord<>(
                    new HiringTraceEvent("VAC-FP-2", "Confirm funding availability", "Head of Finance", 1_700_000_001_000L, false)));
            harness.processElement(new StreamRecord<>(
                    new HiringTraceEvent("VAC-FP-1", "Publish position posting", "HR", 1_700_000_002_000L, false)));

            assertTrue(harness.extractOutputStreamRecords().isEmpty());
            assertEquals("COLLECTING", store.snapshotVacancy("VAC-FP-1").orElseThrow().get("status"));

            harness.processElement(new StreamRecord<>(
                    new HiringTraceEvent("VAC-FP-1", "Close position unfilled", "HR", 1_700_000_005_000L, true)));

            var outputs = harness.extractOutputStreamRecords();
            assertEquals(1, outputs.size());

            VacancyLogReady ready = outputs.get(0).getValue();
            assertEquals("VAC-FP-1", ready.vacancyId);
            assertEquals(3, ready.eventCount);
            assertTrue(Files.isRegularFile(Path.of(ready.xesPath)));
            assertTrue(ready.xesXml.contains("Close position unfilled"));
            assertFalse(ready.xesXml.contains("VAC-FP-2"));
            assertEquals("CHECKING_CONFORMANCE", store.snapshotVacancy("VAC-FP-1").orElseThrow().get("status"));
            assertEquals(ready.xesXml, store.getXesXml("VAC-FP-1"));
        } finally {
            RuntimeRegistry.remove(runtimeId);
        }
    }

    @Test
    void clearsKeyedStateAfterTerminalEventSoSubsequentTraceStartsFresh(@TempDir Path tempDir) throws Exception {
        VacancyTraceStore store = new VacancyTraceStore(false);
        String runtimeId = RuntimeRegistry.register(store, null);
        try (var harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new VacancyTraceCollector(runtimeId, tempDir.toString())),
                (HiringTraceEvent event) -> event.vacancyId,
                Types.STRING)) {
            harness.open();

            harness.processElement(new StreamRecord<>(
                    new HiringTraceEvent("VAC-FP-1", "Confirm funding availability", "Head of Finance", 1_700_000_000_000L, false)));
            harness.processElement(new StreamRecord<>(
                    new HiringTraceEvent("VAC-FP-1", "Close position unfilled", "HR", 1_700_000_001_000L, true)));
            assertEquals(1, harness.extractOutputStreamRecords().size());

            harness.processElement(new StreamRecord<>(
                    new HiringTraceEvent("VAC-FP-1", "Request full professor position opening", "Head of Department", 1_700_000_005_000L, false)));
            assertEquals(1, store.snapshotVacancy("VAC-FP-1").orElseThrow().get("eventCount"));
        } finally {
            RuntimeRegistry.remove(runtimeId);
        }
    }
}
