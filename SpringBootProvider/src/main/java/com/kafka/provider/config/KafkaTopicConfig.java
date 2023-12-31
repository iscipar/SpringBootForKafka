package com.kafka.provider.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic generateTopic() {
        Map<String, String> configurations = new HashMap<>();
        configurations.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
        configurations.put(TopicConfig.RETENTION_MS_CONFIG, "604800000");
        configurations.put(TopicConfig.SEGMENT_BYTES_CONFIG, "1073741824");
        configurations.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "1048588");

        return TopicBuilder.name("spring-boot-topic")
                .configs(configurations)
                .build();
    }

    @Bean
    public NewTopic generateTopicTransactional() {
        Map<String, String> configurations = new HashMap<>();
        configurations.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
        configurations.put(TopicConfig.RETENTION_MS_CONFIG, "604800000");
        configurations.put(TopicConfig.SEGMENT_BYTES_CONFIG, "1073741824");
        configurations.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "1048588");

        return TopicBuilder.name("spring-boot-topic-transactional")
                .configs(configurations)
                .build();
    }

    @Bean
    public NewTopic generateTopicRequest() {
        Map<String, String> configurations = new HashMap<>();
        configurations.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
        configurations.put(TopicConfig.RETENTION_MS_CONFIG, "604800000");
        configurations.put(TopicConfig.SEGMENT_BYTES_CONFIG, "1073741824");
        configurations.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "1048588");

        return TopicBuilder.name("spring-boot-topic-request")
                .configs(configurations)
                .build();
    }

    @Bean
    public NewTopic generateTopicReply() {
        Map<String, String> configurations = new HashMap<>();
        configurations.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
        configurations.put(TopicConfig.RETENTION_MS_CONFIG, "604800000");
        configurations.put(TopicConfig.SEGMENT_BYTES_CONFIG, "1073741824");
        configurations.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "1048588");

        return TopicBuilder.name("spring-boot-topic-reply")
                .configs(configurations)
                .build();
    }

    @Bean
    public NewTopic generateTopicParallel() {
        Map<String, String> configurations = new HashMap<>();
        configurations.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
        configurations.put(TopicConfig.RETENTION_MS_CONFIG, "604800000");
        configurations.put(TopicConfig.SEGMENT_BYTES_CONFIG, "1073741824");
        configurations.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "1048588");

        return TopicBuilder.name("spring-boot-topic-parallel")
                .configs(configurations)
                .build();
    }

    @Bean
    public NewTopic generateTopicAdvice() {
        Map<String, String> configurations = new HashMap<>();
        configurations.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
        configurations.put(TopicConfig.RETENTION_MS_CONFIG, "604800000");
        configurations.put(TopicConfig.SEGMENT_BYTES_CONFIG, "1073741824");
        configurations.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "1048588");

        return TopicBuilder.name("spring-boot-topic-advice")
                .configs(configurations)
                .partitions(3)
                .build();
    }
}
