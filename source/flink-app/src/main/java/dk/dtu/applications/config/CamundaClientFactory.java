package dk.dtu.applications.config;

import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import java.io.Serializable;
import java.net.http.HttpClient;
import java.time.Duration;

public record CamundaClientFactory(String baseUrl, String username, String password, String bearerToken,
                                   Duration connectTimeout, Duration readTimeout) implements Serializable {
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
