package dk.dtu.applications.api;

import dk.dtu.applications.model.ConformanceChecker;
import dk.dtu.applications.model.ConformanceResult;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class ConformanceControllerTest {

    @Test
    void delegatesJsonRawAndMultipartChecksAndExposesSummaryHistory() throws Exception {
        ConformanceResult expected = ConformanceResult.of(
                "Full_Professor_Hiring_Compliance.dcr.xml",
                "vac-1.xes",
                1, 0, 0, 0, "Trace Variant #1");

        AtomicReference<String> capturedLogName = new AtomicReference<>();
        AtomicReference<ConformanceResult> latest = new AtomicReference<>();

        ConformanceChecker stubChecker = new ConformanceChecker() {
            @Override
            public ConformanceResult checkXml(String xesXml, String optionalModelXml, String logName) {
                capturedLogName.set(logName);
                latest.set(expected);
                return expected;
            }

            @Override
            public Optional<ConformanceResult> latestResult() {
                return Optional.ofNullable(latest.get());
            }

            @Override
            public List<ConformanceResult> recentResults() {
                ConformanceResult r = latest.get();
                return r == null ? List.of() : List.of(r);
            }
        };

        ConformanceController controller = new ConformanceController(stubChecker);

        var jsonResp = controller.checkJson(new ConformanceController.CheckRequest("<log/>", null, "vac-1.xes"));
        assertEquals(HttpStatus.OK, jsonResp.getStatusCode());
        assertEquals(expected, jsonResp.getBody());
        assertEquals("vac-1.xes", capturedLogName.get());

        var rawResp = controller.checkRawXml("<log/>");
        assertEquals(HttpStatus.OK, rawResp.getStatusCode());
        assertEquals("raw-upload.xes", capturedLogName.get());

        var multiResp = controller.checkMultipart(multipart("vac-2.xes", "<log/>"), null);
        assertEquals(HttpStatus.OK, multiResp.getStatusCode());
        assertEquals("vac-2.xes", capturedLogName.get());

        var summary = controller.summary();
        assertEquals(HttpStatus.OK, summary.getStatusCode());
        assertEquals(expected, summary.getBody().get("latestResult"));
    }

    @Test
    void rejectsEmptyMultipartUpload() {
        ConformanceController controller = new ConformanceController(new ConformanceChecker());
        MultipartFile emptyFile = multipart("empty.xes", "");
        assertThrows(IllegalArgumentException.class, () -> controller.checkMultipart(emptyFile, null));
    }

    private static MultipartFile multipart(String filename, String content) {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        return new MultipartFile() {
            @Override public String getName() { return "file"; }
            @Override public String getOriginalFilename() { return filename; }
            @Override public String getContentType() { return "application/xml"; }
            @Override public boolean isEmpty() { return bytes.length == 0; }
            @Override public long getSize() { return bytes.length; }
            @Override public byte[] getBytes() { return bytes; }
            @Override public InputStream getInputStream() { return new ByteArrayInputStream(bytes); }
            @Override public void transferTo(File dest) { throw new UnsupportedOperationException(); }
        };
    }
}
