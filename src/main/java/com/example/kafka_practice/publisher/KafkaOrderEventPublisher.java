package com.example.kafka_practice.publisher;

import com.example.kafka_practice.model.OrderCreatedEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * Kafka implementation of EventPublisher for OrderCreatedEvent.
 *
 * SOLID Principles:
 * - SRP: Exclusively handles Kafka message creation, dispatching, and broker ACK/NACK logging.
 * - DIP: Injected as an EventPublisher<OrderCreatedEvent> abstraction into business services.
 */
@Component
public class KafkaOrderEventPublisher implements EventPublisher<OrderCreatedEvent> {

    private static final Logger log = LoggerFactory.getLogger(KafkaOrderEventPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String ordersTopic;

    public KafkaOrderEventPublisher(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${ecommerce.topics.orders-created:orders.created}") String ordersTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.ordersTopic = ordersTopic;
    }

    @Override
    public CompletableFuture<SendResult<String, Object>> publish(String key, OrderCreatedEvent event) {
        ProducerRecord<String, Object> record = new ProducerRecord<>(ordersTopic, key, event);

        log.info("[KAFKA PUBLISHER] Sending OrderCreatedEvent: orderId={}, key='{}'", event.getOrderId(), key);

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(record);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                RecordMetadata md = result.getRecordMetadata();
                log.info("[KAFKA ACK] Order {} written to topic={}, partition={}, offset={}",
                        event.getOrderId(), md.topic(), md.partition(), md.offset());
            } else {
                log.error("[KAFKA NACK] Failed to publish order {}: {}", event.getOrderId(), ex.getMessage(), ex);
            }
        });

        return future;
    }
}
