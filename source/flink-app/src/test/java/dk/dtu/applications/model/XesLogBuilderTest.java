package dk.dtu.applications.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class XesLogBuilderTest {
    @Test
    void buildsEscapedXesAndPublishesIntoNewDirectory(@TempDir Path tempDir) throws Exception {
        XesLogBuilder builder = new XesLogBuilder();
        Instant t1 = Instant.parse("2026-11-18T10:00:00Z");
        Instant t2 = Instant.parse("2026-11-18T11:30:00Z");

        builder.appendEvent("VAC-1 & 2", "Confirm funding availability", "Head of Finance", t1);
        builder.appendEvent("VAC-1 & 2", "Publish \"position\" <posting>", "HR", t2);

        assertEquals(1, builder.traces().size());
        assertEquals(2, builder.traces().get(0).events().size());

        Path target = tempDir.resolve("nested/logs/vac-1.xes");
        builder.publish(target);

        assertTrue(Files.isRegularFile(target));
        String xml = Files.readString(target);
        assertTrue(xml.contains("<string key=\"concept:name\" value=\"VAC-1 &amp; 2\"/>"));
        assertTrue(xml.contains("<string key=\"concept:name\" value=\"Publish &quot;position&quot; &lt;posting&gt;\"/>"));
        assertTrue(xml.contains("<date key=\"time:timestamp\" value=\"2026-11-18T10:00:00.000Z\"/>"));
    }
}
