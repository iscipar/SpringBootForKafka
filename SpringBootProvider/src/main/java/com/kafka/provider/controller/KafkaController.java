package com.kafka.provider.controller;

import com.kafka.bo.dto.AdviceDTO;
import com.kafka.bo.dto.RequestDTO;
import com.kafka.bo.dto.ResponseDTO;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@RestController
public class KafkaController {

    @Autowired
    private ReplyingKafkaTemplate<String, RequestDTO, ResponseDTO> replyingKafkaTemplate;

    @Autowired
    private KafkaTemplate<String, AdviceDTO> kafkaTemplateAdvice;

    private Logger LOGGER = LoggerFactory.getLogger(KafkaController.class);

    @PostMapping("/calculator")
    public ResponseEntity<ResponseDTO> getObject(@RequestBody RequestDTO requestDTO) throws InterruptedException, ExecutionException {
        ProducerRecord<String, RequestDTO> record = new ProducerRecord<>("spring-boot-topic-request", null, UUID.randomUUID().toString(), requestDTO);
        RequestReplyFuture<String, RequestDTO, ResponseDTO> future = replyingKafkaTemplate.sendAndReceive(record);
        ConsumerRecord<String, ResponseDTO> response = future.get();
        return new ResponseEntity<>(response.value(), HttpStatus.OK);
    }

    @PostMapping("/advice")
    public void advice() throws InterruptedException {
        final int messagesPerRequest = 30;
        CountDownLatch latch = new CountDownLatch(messagesPerRequest);
        IntStream.range(0, messagesPerRequest).forEach(i -> kafkaTemplateAdvice.send("spring-boot-topic-advice", String.valueOf(i), new AdviceDTO("Texto del mensaje", i)));
        latch.await(30, TimeUnit.SECONDS);
        LOGGER.info("Se han enviado todos los mensajes");
    }
}
