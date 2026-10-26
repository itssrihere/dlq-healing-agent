package com.example.dlqagent;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class DlqAgentApplication {

    public static void main(String[] args) {
        SpringApplication.run(DlqAgentApplication.class, args);
    }
}
