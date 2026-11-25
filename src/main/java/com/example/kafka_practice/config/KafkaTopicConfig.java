package com.example.kafka_practice.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares Kafka topics programmatically.
 * When Spring Boot connects to Kafka, KafkaAdmin automatically creates any missing topics defined here.
 */
@Configuration
public class KafkaTopicConfig {

    @Value("${ecommerce.topics.orders-created:orders.created}")
    private String ordersTopic;

    @Value("${ecommerce.topics.orders-dlt:orders.created.DLT}")
    private String ordersDltTopic;

    @Value("${ecommerce.topics.payments-events:payments.events}")
    private String paymentsTopic;

    @Value("${ecommerce.topics.inventory-events:inventory.events}")
    private String inventoryTopic;

    /**
     * Primary order topic with 3 partitions to allow concurrent consumers.
     */
    @Bean
    public NewTopic ordersTopic() {
        return TopicBuilder.name(ordersTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    /**
     * Dead Letter Topic for orders that fail repeatedly after retries.
     */
    @Bean
    public NewTopic ordersDltTopic() {
        return TopicBuilder.name(ordersDltTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    /**
     * Payments topic.
     */
    @Bean
    public NewTopic paymentsTopic() {
        return TopicBuilder.name(paymentsTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    /**
     * Inventory events topic.
     */
    @Bean
    public NewTopic inventoryTopic() {
        return TopicBuilder.name(inventoryTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
