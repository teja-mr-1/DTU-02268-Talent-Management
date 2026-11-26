package dk.dtu.applications.dcr;

import java.io.Serializable;
import java.io.StringReader;
import java.net.http.HttpClient;
import java.time.Duration;
import java.time.Instant;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import org.xml.sax.InputSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
public record DcrClient(
        @Value("${dcr.base-url}") String baseUrl,
        @Value("${dcr.username}") String username,
        @Value("${dcr.password}") String password,
        @Value("${spring.http.client.connect-timeout}") Duration connectTimeout,
        @Value("${spring.http.client.read-timeout}") Duration readTimeout) implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(DcrClient.class);

    private RestClient client() {
        // The DCR server requires Content-Length even for empty POSTs; use HTTP/1.1 framing.
        var requests = new JdkClientHttpRequestFactory(HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(connectTimeout).build());
        requests.setReadTimeout(readTimeout);
        return RestClient.builder().baseUrl(baseUrl).requestFactory(requests)
            .defaultHeaders(headers -> headers.setBasicAuth(username, password)).build();
    }

    public void startProcess() {
        log.info("Creating DCR scale-up simulation");
        client().post().uri("/sims").contentLength(0).retrieve().toBodilessEntity();
        log.info("DCR scale-up simulation created");
    }

    public String getNewestProcessId() {
        String xml = client().get().uri("/sims/").accept(MediaType.APPLICATION_XML).retrieve().body(String.class);
        try {
            var factory = DocumentBuilderFactory.newInstance();
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
            factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
            var traces = factory.newDocumentBuilder().parse(new InputSource(new StringReader(xml))).getElementsByTagName("trace");
            String newest = null;
            Instant latest = Instant.MIN;
            for (int i = 0; i < traces.getLength(); i++) {
                var trace = (org.w3c.dom.Element) traces.item(i);
                Instant created = Instant.parse(trace.getAttribute("created"));
                if (created.isAfter(latest)) {
                    latest = created;
                    newest = trace.getAttribute("id");
                }
            }
            if (newest == null || newest.isBlank()) throw new IllegalStateException("DCR returned no simulation ID");
            log.info("Selected newest DCR simulation simulationId={}", newest);
            return newest;
        } catch (Exception failure) {
            throw new IllegalStateException("Could not read DCR simulations", failure);
        }
    }

    public void executeEvent(String simulationId, String event) {
        log.info("Executing DCR event simulationId={} event={}", simulationId, event);
        client().post().uri("/sims/{id}/events/{event}", simulationId, event).contentLength(0).retrieve().toBodilessEntity();
        log.info("DCR event executed simulationId={} event={}", simulationId, event);
    }
}
