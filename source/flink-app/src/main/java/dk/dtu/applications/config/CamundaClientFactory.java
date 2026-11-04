package dk.dtu.applications.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import java.io.Serializable;
import java.net.http.HttpClient;
import java.time.Duration;

@Component
public record CamundaClientFactory(String baseUrl, String username, String password, String bearerToken,
                                   Duration connectTimeout, Duration readTimeout) implements Serializable {
    @Autowired
    public CamundaClientFactory(
            @Value("${spring.http.client.connect-timeout}") Duration connectTimeout,
            @Value("${spring.http.client.read-timeout}") Duration readTimeout) {
        this(Settings.value("CAMUNDA_REST_URL", "http://orchestration:8080/v2"),
            Settings.value("CAMUNDA_USERNAME", ""), Settings.value("CAMUNDA_PASSWORD", ""),
            Settings.value("CAMUNDA_BEARER_TOKEN", ""), connectTimeout, readTimeout);
    }

    public RestClient create() {
        var requests = new JdkClientHttpRequestFactory(HttpClient.newBuilder()
            .connectTimeout(connectTimeout).build());
        requests.setReadTimeout(readTimeout);
        return RestClient.builder().baseUrl(baseUrl).requestFactory(requests)
            .defaultHeaders(headers -> {
                if (!bearerToken.isEmpty()) headers.setBearerAuth(bearerToken);
                else if (!username.isEmpty()) headers.setBasicAuth(username, password);
            }).build();
    }
}
