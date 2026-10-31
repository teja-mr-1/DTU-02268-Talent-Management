package dk.dtu.applications.config;

import dk.dtu.applications.camunda.CamundaSink;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;
import java.time.Duration;

@Configuration(proxyBeanMethods = false)
public class CamundaConfiguration {
    @Bean
    CamundaClientFactory camundaClientFactory(
            @Value("${spring.http.client.connect-timeout}") Duration connectTimeout,
            @Value("${spring.http.client.read-timeout}") Duration readTimeout) {
        return new CamundaClientFactory(
            Settings.value("CAMUNDA_REST_URL", "http://orchestration:8080/v2"),
            Settings.value("CAMUNDA_USERNAME", ""), Settings.value("CAMUNDA_PASSWORD", ""),
            Settings.value("CAMUNDA_BEARER_TOKEN", ""), connectTimeout, readTimeout);
    }

    @Bean
    RestClient camundaRestClient(CamundaClientFactory factory) { return factory.create(); }

    @Bean
    CamundaSink camundaSink(CamundaClientFactory factory) { return new CamundaSink(factory); }
}
