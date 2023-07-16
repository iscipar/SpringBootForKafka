package com.kafka.provider.listeners;

import com.kafka.bo.dto.RequestDTO;
import com.kafka.bo.dto.ResponseDTO;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.messaging.handler.annotation.SendTo;

@Configuration
public class KafkaProviderListener {

    private Logger LOGGER = LoggerFactory.getLogger(KafkaProviderListener.class);

    @KafkaListener(topics = "spring-boot-topic-request", groupId = "group-id-request-reply", properties = { ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG + "=org.springframework.kafka.support.serializer.JsonDeserializer", JsonDeserializer.TRUSTED_PACKAGES + "=*" })
    @SendTo
    public ResponseDTO listenerCalculator(RequestDTO requestDTO) throws NumberFormatException {
        LOGGER.info("Calculando el resultado...");
        ResponseDTO responseDTO = new ResponseDTO();
        responseDTO.setFirstNumber(requestDTO.getFirstNumber());
        responseDTO.setSecondNumber(requestDTO.getSecondNumber());
        responseDTO.setSum(String.valueOf(Integer.valueOf(requestDTO.getFirstNumber()) + Integer.valueOf(requestDTO.getSecondNumber())));
        return responseDTO;
    }
}
