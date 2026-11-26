package dk.dtu.applications.config;

public final class Settings {
    private Settings() {}

    public static String value(String key, String fallback) {
        return System.getenv().getOrDefault(key, fallback);
    }
}
