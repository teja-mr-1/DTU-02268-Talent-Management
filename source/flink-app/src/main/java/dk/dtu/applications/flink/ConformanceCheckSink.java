package dk.dtu.applications.flink;

import dk.dtu.applications.config.Settings;
import dk.dtu.applications.model.ConformanceResult;
import dk.dtu.applications.model.VacancyLogReady;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

/**
 * Step 3 Flink sink: sends each published per-vacancy XES file to Process Doctor
 * via ConformanceChecker and records the compliance verdict.
 */
public final class ConformanceCheckSink implements Sink<VacancyLogReady> {
    private static final Logger log = LoggerFactory.getLogger(ConformanceCheckSink.class);
    private final String runtimeId;

    public ConformanceCheckSink(String runtimeId) {
        this.runtimeId = runtimeId;
    }

    @Override
    public SinkWriter<VacancyLogReady> createWriter(WriterInitContext context) {
        return new Writer(runtimeId);
    }

    private static final class Writer implements SinkWriter<VacancyLogReady> {
        private final String runtimeId;

        Writer(String runtimeId) {
            this.runtimeId = runtimeId;
        }

        @Override
        public void write(VacancyLogReady ready, Context context) {
            RuntimeRegistry.Services services = RuntimeRegistry.get(runtimeId);
            boolean autoCheck = (services != null && services.store() != null)
                    ? services.store().isAutoConformanceCheck()
                    : Boolean.parseBoolean(Settings.value("AUTO_CONFORMANCE_CHECK", "true"));
            if (!autoCheck) {
                log.info("AUTO_CONFORMANCE_CHECK=false: XES log ready for manual upload vacancyId={} path={}",
                        ready.vacancyId, ready.xesPath);
                if (services != null && services.store() != null) {
                    services.store().markReadyForManualUpload(ready.vacancyId);
                }
                return;
            }
            if (services == null || services.checker() == null) {
                log.warn("No ConformanceChecker registered for runtimeId={}, skipping check for {}",
                        runtimeId, ready.vacancyId);
                return;
            }
            try {
                ConformanceResult result = services.checker().check(Path.of(ready.xesPath));
                if (services.store() != null) {
                    services.store().recordConformanceResult(ready.vacancyId, result);
                }
            } catch (RuntimeException error) {
                log.error("Conformance check failed for vacancyId={}: {}", ready.vacancyId, error.getMessage());
                if (services.store() != null) {
                    services.store().recordConformanceError(ready.vacancyId, error.getMessage());
                }
            }
        }

        @Override
        public void flush(boolean endOfInput) {
        }

        @Override
        public void close() {
        }
    }
}
