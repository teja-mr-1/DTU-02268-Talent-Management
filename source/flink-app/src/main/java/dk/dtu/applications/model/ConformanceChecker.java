package dk.dtu.applications.model;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserType;
import com.microsoft.playwright.FileChooser;
import com.microsoft.playwright.Locator;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;
import com.microsoft.playwright.options.AriaRole;
import com.microsoft.playwright.options.WaitForSelectorState;
import dk.dtu.applications.config.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

@Service
public class ConformanceChecker {
    private static final Logger log = LoggerFactory.getLogger(ConformanceChecker.class);
    public static final String DEFAULT_APP_URL = "https://processdoctor.compute.dtu.dk/";
    public static final String DEFAULT_MODEL_RESOURCE = "/dcr/Full_Professor_Hiring_Compliance.dcr.xml";
    private static final List<String> SYSTEM_CHROMIUM = List.of(
            "/usr/bin/chromium", "/usr/bin/chromium-browser", "/usr/bin/google-chrome");

    private final String appUrl;
    private final Path configuredModelPath;
    private final long timeoutMs;

    @Autowired
    public ConformanceChecker() {
        String modelPath = Settings.value("DCR_MODEL_PATH", "").trim();
        this.appUrl = Settings.value("PROCESS_DOCTOR_URL", DEFAULT_APP_URL);
        this.configuredModelPath = modelPath.isEmpty() ? null : Path.of(modelPath);
        this.timeoutMs = Long.parseLong(Settings.value("CONFORMANCE_TIMEOUT_MS", "60000"));
    }

    ConformanceChecker(String appUrl, Path configuredModelPath, long timeoutMs) {
        this.appUrl = appUrl;
        this.configuredModelPath = configuredModelPath;
        this.timeoutMs = timeoutMs;
    }

    public ConformanceResult check(Path logFile) {
        Path tempModel = null;
        try {
            Path modelFile = configuredModelPath != null ? configuredModelPath : (tempModel = extractDefaultModel());
            String modelName = configuredModelPath != null
                    ? configuredModelPath.getFileName().toString()
                    : Path.of(DEFAULT_MODEL_RESOURCE).getFileName().toString();
            return runCheck(modelFile, modelName, logFile);
        } catch (IOException error) {
            throw new RuntimeException("Failed to load DCR compliance model", error);
        } finally {
            if (tempModel != null) {
                try { Files.deleteIfExists(tempModel); } catch (IOException ignored) {}
            }
        }
    }

    public ConformanceResult check(Path modelFile, Path logFile) {
        return runCheck(modelFile, modelFile.getFileName().toString(), logFile);
    }

    private ConformanceResult runCheck(Path modelFile, String modelName, Path logFile) {
        if (modelFile == null || !Files.isRegularFile(modelFile)) {
            throw new IllegalArgumentException("DCR model file not found: " + modelFile);
        }
        if (logFile == null || !Files.isRegularFile(logFile)) {
            throw new IllegalArgumentException("XES log file not found: " + logFile);
        }
        String logName = logFile.getFileName().toString();
        log.info("Running Process Doctor conformance check model={} log={}", modelName, logName);

        try (Playwright pw = Playwright.create();
             Browser browser = pw.chromium().launch(launchOptions())) {
            Page page = browser.newPage();
            page.navigate(appUrl);
            page.getByText("Conformance").click();
            page.getByText("Open Model").click();

            FileChooser modelChooser = page.waitForFileChooser(() -> page.getByText("Open Editor XML").click());
            modelChooser.setFiles(modelFile);
            page.waitForTimeout(500);

            FileChooser logChooser = page.waitForFileChooser(() -> page.getByText("Upload Log").click());
            logChooser.setFiles(logFile);
            page.waitForTimeout(500);
            page.getByText("Check!").click();

            Locator resultsPanel = page.getByRole(AriaRole.LIST)
                    .filter(new Locator.FilterOptions().setHasText("Trace Variant #1"));
            resultsPanel.waitFor(new Locator.WaitForOptions()
                    .setState(WaitForSelectorState.VISIBLE)
                    .setTimeout((double) timeoutMs));

            String resultText = resultsPanel.innerText();
            int conforming = parseCount(page.getByTitle("Conforming", new Page.GetByTitleOptions().setExact(true)).first().innerText());
            int partial = parseCount(page.getByTitle("Partially Violating", new Page.GetByTitleOptions().setExact(true)).first().innerText());
            int violating = parseCount(page.getByTitle("Violating", new Page.GetByTitleOptions().setExact(true)).first().innerText());
            int structural = parseCount(page.getByTitle("Total Structural Violations", new Page.GetByTitleOptions().setExact(true)).first().innerText());

            ConformanceResult result = ConformanceResult.of(modelName, logName, conforming, partial, violating, structural, resultText);
            log.info("Conformance result log={} conformant={} conforming={} partial={} violating={} structural={}",
                    logName, result.conformant(), conforming, partial, violating, structural);
            return result;
        }
    }

    private static BrowserType.LaunchOptions launchOptions() {
        BrowserType.LaunchOptions options = new BrowserType.LaunchOptions().setHeadless(true);
        if (!hasPlaywrightCache()) {
            for (String path : SYSTEM_CHROMIUM) {
                if (Files.isExecutable(Path.of(path))) {
                    return options.setExecutablePath(Path.of(path));
                }
            }
        }
        return options;
    }

    static boolean hasPlaywrightCache() {
        String custom = Settings.value("PLAYWRIGHT_BROWSERS_PATH", "");
        Path dir = !custom.isBlank() ? Path.of(custom) : Path.of(System.getProperty("user.home", ""), ".cache", "ms-playwright");
        if (!Files.isDirectory(dir)) {
            return false;
        }
        try (var s = Files.list(dir)) {
            return s.anyMatch(Files::isDirectory);
        } catch (IOException ignored) {
            return false;
        }
    }

    private static Path extractDefaultModel() throws IOException {
        try (InputStream stream = ConformanceChecker.class.getResourceAsStream(DEFAULT_MODEL_RESOURCE)) {
            if (stream == null) {
                throw new IOException("Bundled DCR model not found: " + DEFAULT_MODEL_RESOURCE);
            }
            Path temp = Files.createTempFile("dcr-model-", ".dcr.xml");
            Files.write(temp, stream.readAllBytes());
            return temp;
        }
    }

    static int parseCount(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("Blank count value from Process Doctor");
        }
        return Integer.parseInt(raw.trim().split("\\s+")[0]);
    }
}
