package com.kafka.provider.controller;

import com.kafka.bo.dto.RequestDTO;
import com.kafka.bo.dto.ResponseDTO;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

@RestController
public class KafkaController {

    @Autowired
    private ReplyingKafkaTemplate<String, RequestDTO, ResponseDTO> replyingKafkaTemplate;

    @PostMapping("/calculator")
    public ResponseEntity<ResponseDTO> getObject(@RequestBody RequestDTO requestDTO) throws InterruptedException, ExecutionException {
        ProducerRecord<String, RequestDTO> record = new ProducerRecord<>("spring-boot-topic-request", null, UUID.randomUUID().toString(), requestDTO);
        RequestReplyFuture<String, RequestDTO, ResponseDTO> future = replyingKafkaTemplate.sendAndReceive(record);
        ConsumerRecord<String, ResponseDTO> response = future.get();
        return new ResponseEntity<>(response.value(), HttpStatus.OK);
    }
}
