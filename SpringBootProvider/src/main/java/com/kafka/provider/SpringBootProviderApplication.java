package com.kafka.provider;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@SpringBootApplication
public class SpringBootProviderApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringBootProviderApplication.class, args);
	}

	@Bean
	CommandLineRunner init(KafkaTemplate<String, String> kafkaTemplate, KafkaTemplate<String, String> kafkaTemplateTransactional, KafkaTransactionManager kafkaTransactionManager) {
		return args -> {
			kafkaTemplate.send("spring-boot-topic", "Mensaje 1 enviado desde Spring Boot");
			kafkaTemplate.send("spring-boot-topic", "Mensaje 2 enviado desde Spring Boot");
			kafkaTemplate.send("spring-boot-topic", "Mensaje 3 enviado desde Spring Boot");
			kafkaTemplate.send("spring-boot-topic", "Mensaje 4 enviado desde Spring Boot");
			kafkaTemplate.send("spring-boot-topic", "Mensaje 5 enviado desde Spring Boot");
			kafkaTemplate.send("spring-boot-topic", "Mensaje 6 enviado desde Spring Boot");
			kafkaTemplate.send("spring-boot-topic", "");
			kafkaTemplate.send("spring-boot-topic", "Error");
			TransactionTemplate transactionTemplate = new TransactionTemplate(kafkaTransactionManager);
			transactionTemplate.execute(s -> {
				kafkaTemplateTransactional.send("spring-boot-topic-transactional", "Mensaje enviado desde Spring Boot a través de una transacción");
				return null;
			});
		};
	}
}
