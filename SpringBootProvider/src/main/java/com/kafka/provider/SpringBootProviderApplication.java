package com.kafka.provider;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class SpringBootProviderApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringBootProviderApplication.class, args);
	}

	@Bean
	CommandLineRunner init(KafkaTemplate<String, String> kafkaTemplate) {
		return args -> {
			kafkaTemplate.send("spring-boot-topic", "Mensaje 1 enviado desde Spring Boot");
			kafkaTemplate.send("spring-boot-topic", "Mensaje 2 enviado desde Spring Boot");
			kafkaTemplate.send("spring-boot-topic", "Mensaje 3 enviado desde Spring Boot");
			kafkaTemplate.send("spring-boot-topic", "Mensaje 4 enviado desde Spring Boot");
			kafkaTemplate.send("spring-boot-topic", "Mensaje 5 enviado desde Spring Boot");
			kafkaTemplate.send("spring-boot-topic", "Mensaje 6 enviado desde Spring Boot");
			kafkaTemplate.send("spring-boot-topic", "");
		};
	}
}
