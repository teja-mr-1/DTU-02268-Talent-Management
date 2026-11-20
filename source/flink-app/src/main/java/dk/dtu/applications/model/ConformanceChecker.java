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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Runs XES event logs against a DCR compliance model using DTU Process Doctor
 * via headless Chromium (Playwright) and retains recent conformance results.
 */
@Service
public class ConformanceChecker {
    private static final Logger log = LoggerFactory.getLogger(ConformanceChecker.class);
    public static final String DEFAULT_APP_URL = "https://processdoctor.compute.dtu.dk/";
    public static final String DEFAULT_MODEL_RESOURCE = "/dcr/Full_Professor_Hiring_Compliance.dcr.xml";
    private static final int MAX_HISTORY = 20;
    private static final List<String> SYSTEM_CHROMIUM_PATHS = List.of(
            "/usr/bin/chromium",
            "/usr/bin/chromium-browser",
            "/usr/bin/google-chrome"
    );

    private final String appUrl;
    private final Path configuredModelPath;
    private final long timeoutMs;
    private final boolean headless;
    private final AtomicReference<ConformanceResult> latestResult = new AtomicReference<>();
    private final Deque<ConformanceResult> history = new ArrayDeque<>();

    @Autowired
    public ConformanceChecker() {
        this(
                Settings.value("PROCESS_DOCTOR_URL", DEFAULT_APP_URL),
                optionalPath(Settings.value("DCR_MODEL_PATH", "")),
                parseLong(Settings.value("CONFORMANCE_TIMEOUT_MS", "60000"), 60_000L),
                Boolean.parseBoolean(Settings.value("PLAYWRIGHT_HEADLESS", "true"))
        );
    }

    ConformanceChecker(String appUrl, Path configuredModelPath, long timeoutMs, boolean headless) {
        this.appUrl = appUrl;
        this.configuredModelPath = configuredModelPath;
        this.timeoutMs = timeoutMs;
        this.headless = headless;
    }

    /** Check an XES file against the configured or bundled default DCR compliance model. */
    public ConformanceResult check(Path logFile) {
        String logName = logFile != null && logFile.getFileName() != null
                ? logFile.getFileName().toString()
                : "trace.xes";
        return checkWithConfiguredModel(logFile, logName);
    }

    /** Check an XES file against an explicit DCR XML model file. */
    public ConformanceResult check(Path modelFile, Path logFile) {
        return executeCheck(
                modelFile,
                modelFile.getFileName().toString(),
                logFile,
                logFile.getFileName().toString()
        );
    }

    /** Publish an in-memory XesLogBuilder to a temporary file and verify conformance. */
    public ConformanceResult check(XesLogBuilder logBuilder) {
        return check(logBuilder, "in-memory-trace.xes");
    }

    /** Publish an in-memory XesLogBuilder under a readable log name and verify conformance. */
    public ConformanceResult check(XesLogBuilder logBuilder, String logName) {
        Path tempLog = null;
        try {
            tempLog = Files.createTempFile("conformance-log-", ".xes");
            logBuilder.publish(tempLog);
            return checkWithConfiguredModel(tempLog, logName);
        } catch (IOException error) {
            throw new ConformanceCheckException("Failed to write temporary XES log for conformance check", error);
        } finally {
            deleteQuietly(tempLog);
        }
    }

    private ConformanceResult checkWithConfiguredModel(Path logFile, String logName) {
        Path tempModel = null;
        try {
            Path modelFile = configuredModelPath;
            String modelName;
            if (modelFile == null) {
                tempModel = extractDefaultModel();
                modelFile = tempModel;
                modelName = Path.of(DEFAULT_MODEL_RESOURCE).getFileName().toString();
            } else {
                modelName = modelFile.getFileName().toString();
            }
            return executeCheck(modelFile, modelName, logFile, logName);
        } catch (IOException error) {
            throw new ConformanceCheckException("Failed to prepare DCR compliance model", error);
        } finally {
            deleteQuietly(tempModel);
        }
    }

    /** Check raw XES XML content (and optional DCR model XML override) directly. */
    public ConformanceResult checkXml(String xesXml, String optionalModelXml, String logName) {
        if (xesXml == null || xesXml.isBlank()) {
            throw new IllegalArgumentException("XES XML content must not be empty");
        }
        Path tempLog = null;
        Path tempModel = null;
        try {
            tempLog = Files.createTempFile("conformance-log-", ".xes");
            Files.writeString(tempLog, xesXml, StandardCharsets.UTF_8);

            Path modelFile;
            String modelName;
            if (optionalModelXml != null && !optionalModelXml.isBlank()) {
                tempModel = Files.createTempFile("conformance-model-", ".dcr.xml");
                Files.writeString(tempModel, optionalModelXml, StandardCharsets.UTF_8);
                modelFile = tempModel;
                modelName = "custom-model.dcr.xml";
            } else if (configuredModelPath != null) {
                modelFile = configuredModelPath;
                modelName = configuredModelPath.getFileName().toString();
            } else {
                tempModel = extractDefaultModel();
                modelFile = tempModel;
                modelName = Path.of(DEFAULT_MODEL_RESOURCE).getFileName().toString();
            }
            String effectiveLogName = (logName == null || logName.isBlank()) ? "uploaded.xes" : logName;
            return executeCheck(modelFile, modelName, tempLog, effectiveLogName);
        } catch (IOException error) {
            throw new ConformanceCheckException("Failed to stage temporary XML files for conformance check", error);
        } finally {
            deleteQuietly(tempLog);
            deleteQuietly(tempModel);
        }
    }

    public Optional<ConformanceResult> latestResult() {
        return Optional.ofNullable(latestResult.get());
    }

    public List<ConformanceResult> recentResults() {
        synchronized (history) {
            return List.copyOf(history);
        }
    }

    public void clearHistory() {
        latestResult.set(null);
        synchronized (history) {
            history.clear();
        }
    }

    public String configuredModelName() {
        return configuredModelPath != null
                ? configuredModelPath.toString()
                : "classpath:" + DEFAULT_MODEL_RESOURCE;
    }

    private ConformanceResult executeCheck(Path modelFile, String modelName, Path logFile, String logName) {
        validateReadableFile(modelFile, "DCR model");
        validateReadableFile(logFile, "XES log");

        log.info("Running Process Doctor conformance check url={} model={} log={}", appUrl, modelName, logName);

        try (Playwright pw = Playwright.create();
             Browser browser = pw.chromium().launch(buildLaunchOptions())) {
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
            int conforming = readTitleCount(page, "Conforming");
            int partiallyViolating = readTitleCount(page, "Partially Violating");
            int violating = readTitleCount(page, "Violating");
            int structuralViolations = readTitleCount(page, "Total Structural Violations");

            ConformanceResult result = ConformanceResult.of(
                    modelName,
                    logName,
                    conforming,
                    partiallyViolating,
                    violating,
                    structuralViolations,
                    resultText
            );
            record(result);
            log.info("Conformance check finished log={} conformant={} conforming={} partial={} violating={} structural={}",
                    logName, result.conformant(), conforming, partiallyViolating, violating, structuralViolations);
            return result;
        } catch (ConformanceCheckException error) {
            throw error;
        } catch (RuntimeException error) {
            throw new ConformanceCheckException("Process Doctor conformance check failed for log " + logName, error);
        }
    }

    private void record(ConformanceResult result) {
        latestResult.set(result);
        synchronized (history) {
            history.addFirst(result);
            while (history.size() > MAX_HISTORY) {
                history.removeLast();
            }
        }
    }

    private BrowserType.LaunchOptions buildLaunchOptions() {
        BrowserType.LaunchOptions options = new BrowserType.LaunchOptions().setHeadless(headless);
        String explicitExecutable = Settings.value("PLAYWRIGHT_CHROMIUM_EXECUTABLE", "");
        if (!explicitExecutable.isBlank()) {
            options.setExecutablePath(Path.of(explicitExecutable));
            return options;
        }
        String explicitChannel = Settings.value("PLAYWRIGHT_BROWSER_CHANNEL", "");
        if (!explicitChannel.isBlank()) {
            options.setChannel(explicitChannel);
            return options;
        }
        if (!hasPlaywrightBrowserCache()) {
            for (String candidate : SYSTEM_CHROMIUM_PATHS) {
                Path path = Path.of(candidate);
                if (Files.isExecutable(path)) {
                    log.debug("Using system Chromium binary at {}", path);
                    options.setExecutablePath(path);
                    return options;
                }
            }
        }
        return options;
    }

    static boolean hasPlaywrightBrowserCache() {
        String customPath = Settings.value("PLAYWRIGHT_BROWSERS_PATH", "");
        if (!customPath.isBlank() && Files.isDirectory(Path.of(customPath))) {
            return hasDirectories(Path.of(customPath));
        }
        String home = System.getProperty("user.home", "");
        if (!home.isBlank()) {
            Path cache = Path.of(home, ".cache", "ms-playwright");
            return Files.isDirectory(cache) && hasDirectories(cache);
        }
        return false;
    }

    private static boolean hasDirectories(Path root) {
        try (var stream = Files.list(root)) {
            return stream.anyMatch(Files::isDirectory);
        } catch (IOException ignored) {
            return false;
        }
    }

    private Path extractDefaultModel() throws IOException {
        try (InputStream stream = ConformanceChecker.class.getResourceAsStream(DEFAULT_MODEL_RESOURCE)) {
            if (stream == null) {
                throw new IOException("Bundled DCR compliance model not found at " + DEFAULT_MODEL_RESOURCE);
            }
            Path temp = Files.createTempFile("dcr-compliance-model-", ".dcr.xml");
            Files.write(temp, stream.readAllBytes());
            return temp;
        }
    }

    private static int readTitleCount(Page page, String exactTitle) {
        String raw = page.getByTitle(exactTitle, new Page.GetByTitleOptions().setExact(true))
                .first()
                .innerText();
        return parseCount(raw);
    }

    static int parseCount(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new ConformanceCheckException("Process Doctor returned blank count value");
        }
        String firstToken = raw.trim().split("\\s+")[0];
        try {
            return Integer.parseInt(firstToken);
        } catch (NumberFormatException error) {
            throw new ConformanceCheckException("Unexpected count format from Process Doctor: '" + raw + "'", error);
        }
    }

    private static void validateReadableFile(Path file, String label) {
        if (file == null || !Files.isRegularFile(file) || !Files.isReadable(file)) {
            throw new IllegalArgumentException(label + " file does not exist or is not readable: " + file);
        }
    }

    private static void deleteQuietly(Path path) {
        if (path != null) {
            try {
                Files.deleteIfExists(path);
            } catch (IOException ignored) {
            }
        }
    }

    private static Path optionalPath(String value) {
        return (value == null || value.isBlank()) ? null : Path.of(value.trim());
    }

    private static long parseLong(String value, long fallback) {
        try {
            return Long.parseLong(value.trim());
        } catch (RuntimeException ignored) {
            return fallback;
        }
    }

    public static final class ConformanceCheckException extends RuntimeException {
        public ConformanceCheckException(String message) {
            super(message);
        }

        public ConformanceCheckException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
