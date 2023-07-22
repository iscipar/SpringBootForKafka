package com.kafka.provider.config;

import com.kafka.bo.dto.AdviceDTO;
import com.kafka.bo.dto.RequestDTO;
import com.kafka.bo.dto.ResponseDTO;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProviderConfig {

    @Value("${spring.kafka.bootstrapServers}")
    private String bootstrapServers;

    public Map<String, Object> producerConfig() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return properties;
    }

    public Map<String, Object> producerConfigTransactional() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction-id");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 2);
        return properties;
    }

    public Map<String, Object> producerConfigJson() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return properties;
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfig());
    }

    @Bean
    public ProducerFactory<String, String> producerFactoryTransactional() {
        return new DefaultKafkaProducerFactory<>(producerConfigTransactional());
    }

    @Bean
    public ProducerFactory<String, RequestDTO> producerFactoryRequest() {
        return new DefaultKafkaProducerFactory<>(producerConfigJson());
    }

    @Bean
    public ProducerFactory<String, ResponseDTO> producerFactoryResponse() {
        return new DefaultKafkaProducerFactory<>(producerConfigJson());
    }

    @Bean
    public ProducerFactory<String, AdviceDTO> producerFactoryAdvice() {
        return new DefaultKafkaProducerFactory<>(producerConfigJson());
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplateTransactional() {
        return new KafkaTemplate<>(producerFactoryTransactional());
    }

    @Bean
    public KafkaTransactionManager kafkaTransactionManager() {
        return new KafkaTransactionManager<>(producerFactoryTransactional());
    }

    @Bean
    public ReplyingKafkaTemplate<String, RequestDTO, ResponseDTO> replyingKafkaTemplate(ProducerFactory<String, RequestDTO> producerFactoryRequest, ConcurrentKafkaListenerContainerFactory<String, ResponseDTO> factory) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        properties.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        ConsumerFactory<String, ResponseDTO> consumerFactory = new DefaultKafkaConsumerFactory<>(properties);
        factory.setConsumerFactory(consumerFactory);
        ConcurrentMessageListenerContainer<String, ResponseDTO> replyContainer = factory.createContainer("spring-boot-topic-reply");
        replyContainer.getContainerProperties().setMissingTopicsFatal(false);
        replyContainer.getContainerProperties().setGroupId("group-id-request-reply");
        return new ReplyingKafkaTemplate<>(producerFactoryRequest, replyContainer);
    }
    
    @Bean
    public KafkaTemplate<String, ResponseDTO> replyTemplate(ProducerFactory<String, ResponseDTO> producerFactoryResponse, ConcurrentKafkaListenerContainerFactory<String, ResponseDTO> factory) {
        KafkaTemplate<String, ResponseDTO> kafkaTemplate = new KafkaTemplate<>(producerFactoryResponse);
        factory.getContainerProperties().setMissingTopicsFatal(false);
        factory.setReplyTemplate(kafkaTemplate);
        return kafkaTemplate;
    }

    @Bean
    public KafkaTemplate<String, AdviceDTO> kafkaTemplateAdvice() {
        return new KafkaTemplate<>(producerFactoryAdvice());
    }
}
