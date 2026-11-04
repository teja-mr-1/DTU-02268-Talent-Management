package dk.dtu.applications;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class TalentApplication {
    public static void main(String[] args) { SpringApplication.run(TalentApplication.class, args); }

}
