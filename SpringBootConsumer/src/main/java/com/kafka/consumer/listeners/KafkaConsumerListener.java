package com.kafka.consumer.listeners;

import com.kafka.bo.dto.AdviceDTO;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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

    @KafkaListener(topics = {"spring-boot-topic-parallel"}, groupId = "group-id-parallel", containerFactory = "consumerParallel")
    public void listenerParallel(String message) {
        LOGGER.info("Mensaje recibido: " + message);
    }

    @KafkaListener(topics = {"spring-boot-topic-advice"}, groupId = "group-id-advice", containerFactory = "consumerJson")
    public void listenerJson(ConsumerRecord<String, AdviceDTO> consumerRecord) {
        LOGGER.info("Logger [JSON] | clave recibida {} | valor recibido {}", consumerRecord.key(), consumerRecord.value());
    }

    @KafkaListener(topics = {"spring-boot-topic-advice"}, groupId = "group-id-advice", containerFactory = "consumerString")
    public void listenerString(ConsumerRecord<String, String> consumerRecord) {
        LOGGER.info("Logger [String] | clave recibida {} | valor recibido {}", consumerRecord.key(), consumerRecord.value());
    }

    @KafkaListener(topics = {"spring-boot-topic-advice"}, groupId = "group-id-advice", containerFactory = "consumerByteArray")
    public void listenerByteArray(ConsumerRecord<String, byte[]> consumerRecord) {
        LOGGER.info("Logger [ByteArray] | clave recibida {} | valor recibido {}", consumerRecord.key(), consumerRecord.value());
    }
}
