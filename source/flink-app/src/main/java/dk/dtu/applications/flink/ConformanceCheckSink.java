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

public final class ConformanceCheckSink implements Sink<VacancyLogReady> {
    private static final Logger log = LoggerFactory.getLogger(ConformanceCheckSink.class);
    private final String runtimeId;

    public ConformanceCheckSink(String runtimeId) {
        this.runtimeId = runtimeId;
    }

    @Override
    public SinkWriter<VacancyLogReady> createWriter(WriterInitContext context) {
        return new SinkWriter<>() {
            @Override
            public void write(VacancyLogReady ready, Context ctx) {
                RuntimeRegistry.Services services = RuntimeRegistry.get(runtimeId);
                if (!Boolean.parseBoolean(Settings.value("AUTO_CONFORMANCE_CHECK", "true"))) {
                    log.info("AUTO_CONFORMANCE_CHECK=false: XES saved at {}", ready.xesPath);
                    if (services != null && services.store() != null) {
                        services.store().markXesReady(ready.vacancyId);
                    }
                    return;
                }
                if (services == null || services.checker() == null) {
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

            @Override public void flush(boolean endOfInput) {}
            @Override public void close() {}
        };
    }
}
