package dk.dtu.applications.model;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds an XES 1.0 log (openxes style) in memory. Events and traces can be
 * added repeatedly via addTrace / appendEvent; call publish() only when the
 * log is complete to write it to disk atomically.
 */
public final class XesLogBuilder {

    // ---- Simple value objects -------------------------------------------------

    public record XesEvent(String name, String role, Instant timestamp) {
    }

    public static final class XesTrace {
        private final String name;
        private final List<XesEvent> events = new ArrayList<>();

        public XesTrace(String name) {
            this.name = name;
        }

        public String name() {
            return name;
        }

        public List<XesEvent> events() {
            return events;
        }
    }

    // ---- Builder state ---------------------------------------------------------

    private final Map<String, String> globalAttributes = new LinkedHashMap<>();
    private String classifierName = "Event Name";
    private String classifierKey = "concept:name";
    private final List<XesTrace> traces = new ArrayList<>();

    public XesLogBuilder() {
        // Default globals as in the sample; both are marked __INVALID__ because
        // they vary per event/trace and must not be fixed at log level.
        globalAttributes.put("concept:name", "__INVALID__");
        globalAttributes.put("role", "__INVALID__");
    }

    /** Override a global attribute (e.g. xes:annotation or org). */
    public XesLogBuilder globalAttribute(String key, String value) {
        globalAttributes.put(key, value);
        return this;
    }

    public XesLogBuilder classifier(String name, String key) {
        this.classifierName = name;
        this.classifierKey = key;
        return this;
    }

    /** Add a new (initially empty) trace and return it so events can be added. */
    public synchronized XesTrace addTrace(String name) {
        XesTrace trace = new XesTrace(name);
        traces.add(trace);
        return trace;
    }

    /** Find an existing trace by name or create one if it does not exist yet. */
    public synchronized XesTrace getOrAddTrace(String name) {
        for (XesTrace existing : traces) {
            if (existing.name().equals(name)) {
                return existing;
            }
        }
        return addTrace(name);
    }

    /** Append an event to the given trace */
    public synchronized void appendEvent(XesTrace trace, String eventName, String role, Instant timestamp) {
        trace.events().add(new XesEvent(eventName, role, timestamp));
    }

    /** Append an event to the named trace, creating the trace if needed. */
    public synchronized void appendEvent(String traceName, String eventName, String role, Instant timestamp) {
        appendEvent(getOrAddTrace(traceName), eventName, role, timestamp);
    }

    /** Snapshot of currently buffered traces. */
    public synchronized List<XesTrace> traces() {
        return List.copyOf(traces);
    }

    // ---- Serialization ----------------------------------------------------------

    private static final DateTimeFormatter ISO_Z = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
            .withZone(ZoneOffset.UTC);

    public synchronized String toXml() {
        StringBuilder sb = new StringBuilder();
        sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        sb.append("<log xes.version=\"1.0\" xes.features=\"nested-attributes\" openxes.version=\"1.0RC7\">\n");

        if (!globalAttributes.isEmpty()) {
            sb.append("  <global scope=\"event\">\n");
            globalAttributes.forEach((k, v) -> sb.append("    <string key=\"").append(esc(k))
                    .append("\" value=\"").append(esc(v)).append("\"/>\n"));
            sb.append("  </global>\n");
        }

        sb.append("  <classifier name=\"").append(esc(classifierName))
                .append("\" keys=\"").append(esc(classifierKey)).append("\"/>\n");

        for (XesTrace trace : traces) {
            sb.append("  <trace>\n");
            sb.append("    <string key=\"concept:name\" value=\"").append(esc(trace.name())).append("\"/>\n");
            for (XesEvent e : trace.events()) {
                sb.append("    <event>\n");
                sb.append("      <string key=\"concept:name\" value=\"").append(esc(e.name())).append("\"/>\n");
                sb.append("      <string key=\"role\" value=\"").append(esc(e.role())).append("\"/>\n");
                sb.append("      <date key=\"time:timestamp\" value=\"").append(ISO_Z.format(e.timestamp()))
                        .append("\"/>\n");
                sb.append("    </event>\n");
            }
            sb.append("  </trace>\n");
        }

        sb.append("</log>\n");
        return sb.toString();
    }

    /**
     * Publish the finished log. Writes to a temp file first and moves it into
     * place atomically, so readers never see a half-written log.
     */
    public void publish(Path target) throws IOException {
        Path parent = target.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Path tmp = Files.createTempFile(parent, ".xes-", ".tmp");
        try {
            Files.writeString(tmp, toXml(), StandardCharsets.UTF_8);
            Files.move(tmp, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                    java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        } finally {
            Files.deleteIfExists(tmp); // no-op if the move succeeded
        }
    }

    private static String esc(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("&", "&amp;").replace("<", "&lt;")
                .replace(">", "&gt;").replace("\"", "&quot;");
    }
}