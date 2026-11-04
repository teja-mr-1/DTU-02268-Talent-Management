package dk.dtu.applications.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

@Configuration(proxyBeanMethods = false)
public class CamundaConfiguration {
    @Bean
    RestClient camundaRestClient(CamundaClientFactory factory) { return factory.create(); }
}
