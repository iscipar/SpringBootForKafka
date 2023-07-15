package com.kafka.consumer.listeners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

@Configuration
public class KafkaConsumerListener {

    private Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerListener.class);

    @KafkaListener(topics = {"spring-boot-topic"}, groupId = "group-id1", containerFactory = "consumer")
    public void listener1(String message) {
        LOGGER.info("Mensaje recibido por consumer de group-id1: " + message);
    }

    @KafkaListener(topics = {"spring-boot-topic"}, groupId = "group-id2", containerFactory = "consumer")
    public void listener2(String message) {
        LOGGER.info("Mensaje recibido por consumer de group-id2: " + message);
    }

    @KafkaListener(topics = {"spring-boot-topic-transactional"}, groupId = "group-id-transactional", containerFactory = "consumerTransactional")
    public void listenerTransactional(String message) {
        LOGGER.info("Mensaje recibido: " + message);
    }
}
