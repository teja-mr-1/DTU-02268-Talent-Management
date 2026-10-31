package dk.dtu.applications;

import dk.dtu.applications.flink.ApplicationInbox;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class TalentApplication {
    public static void main(String[] args) { SpringApplication.run(TalentApplication.class, args); }

    @Bean ApplicationInbox applicationInbox(@Value("${intake.capacity:1000}") int capacity) {
        return new ApplicationInbox(capacity);
    }
}
