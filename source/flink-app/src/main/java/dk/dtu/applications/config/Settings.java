package dk.dtu.applications.config;

public final class Settings {
    private Settings() {}

    public static String value(String key, String fallback) {
        return System.getenv().getOrDefault(key, fallback);
    }

    public static boolean localCamundaSimulator() {
        String explicit = System.getenv("LOCAL_CAMUNDA_SIMULATOR");
        if (explicit != null && !explicit.isBlank()) {
            return Boolean.parseBoolean(explicit.trim());
        }
        String camundaUrl = System.getenv("CAMUNDA_REST_URL");
        return camundaUrl == null || camundaUrl.isBlank();
    }
}
