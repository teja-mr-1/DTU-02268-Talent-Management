package dk.dtu.applications.model;

import java.nio.file.Path;

import com.microsoft.playwright.*;
import com.microsoft.playwright.options.AriaRole;
import com.microsoft.playwright.options.WaitForSelectorState;

public class ConformanceChecker {
    // ── Configuration ──────────────────────────────────────────────
    private static final String APP_URL = "https://processdoctor.compute.dtu.dk/";
    private static final Path MODEL_FILE = Path.of("/home/amil/Courses/POEDS/pizza.xml"); // TODO: don't hardcode
    private static final Path LOG_FILE = Path.of("/home/amil/Courses/POEDS/pizza.xes"); // TODO: don't hardcode

    public static void main(String[] args) {
        try (Playwright pw = Playwright.create()) {

            Browser browser = pw.chromium().launch(
                    new BrowserType.LaunchOptions().setHeadless(true));
            Page page = browser.newPage();

            // ── 1. Navigate to the app ──────────────────────────────
            System.out.println("→ Navigating to " + APP_URL);
            page.navigate(APP_URL);

            // ── 2. Select the "Conformance" option ──────────────────
            System.out.println("→ Selecting Conformance");
            page.getByText("Conformance").click();

            // ── 3. Open the "Open Model" dropdown ───────────────────
            System.out.println("→ Opening 'Open Model' dropdown");
            page.getByText("Open Model").click();

            // ── 4. Select "Open Editor XML" → triggers file picker ──
            System.out.println("→ Selecting 'Open Editor XML'");
            FileChooser fileChooser = page.waitForFileChooser(() -> {
                page.getByText("Open Editor XML").click();
            });
            fileChooser.setFiles(MODEL_FILE);

            page.waitForTimeout(1_000);

            // ── 5. Click "Upload Log" → second file picker ──────────
            System.out.println("→ Uploading log file");
            fileChooser = page.waitForFileChooser(() -> {
                page.getByText("Upload Log").click();
            });
            fileChooser.setFiles(LOG_FILE);

            page.waitForTimeout(1_000);

            // ── 6. Click "Check" ────────────────────────────────────
            System.out.println("→ Running conformance check");
            page.getByText("Check!").click();

            // ── 7. Wait for results in the side panel ───────────────
            System.out.println("→ Waiting for results…");

            Locator resultsPanel = page.getByRole(AriaRole.LIST)
                    .filter(new Locator.FilterOptions().setHasText("Trace Variant #1"));
            resultsPanel.waitFor(new Locator.WaitForOptions()
                    .setState(WaitForSelectorState.VISIBLE)
                    .setTimeout(60_000));

            // ── 8. Extract the results ──────────────────────────────
            String resultText = resultsPanel.innerText();
            System.out.println("\n═══ RESULTS ═══");
            System.out.println(resultText);

            String conformingCount = page.getByTitle("Conforming").first().innerText().split("\\s+")[0];
            String partiallyViolatingCount = page.getByTitle("Partially Violating").first().innerText()
                    .split("\\s+")[0];
            String violatingCount = page.getByTitle("Violating").first().innerText().split("\\s+")[0];
            String structuralViolationCount = page.getByTitle("Total Structural Violations").first().innerText()
                    .split("\\s+")[0];

            System.out.println(String.format("Conforming: %s", conformingCount));
            System.out.println(String.format("Partially violating: %s", partiallyViolatingCount));
            System.out.println(String.format("Violating: %s", violatingCount));
            System.out.println(String.format("Total Structural Violations: %s", structuralViolationCount));

            browser.close();
        }
    }
}
