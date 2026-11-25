package com.adityapatil.eventbus.publisher;

import com.adityapatil.eventbus.model.PaymentEvent;
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
 * Kafka implementation of EventPublisher for PaymentEvent.
 */
@Component
public class KafkaPaymentEventPublisher implements EventPublisher<PaymentEvent> {

    private static final Logger log = LoggerFactory.getLogger(KafkaPaymentEventPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String paymentsTopic;

    public KafkaPaymentEventPublisher(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${ecommerce.topics.payments-events:payments.events}") String paymentsTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.paymentsTopic = paymentsTopic;
    }

    @Override
    public CompletableFuture<SendResult<String, Object>> publish(String key, PaymentEvent event) {
        ProducerRecord<String, Object> record = new ProducerRecord<>(paymentsTopic, key, event);

        log.info("[KAFKA PUBLISHER] Sending PaymentEvent: paymentId={}, status={}", event.getPaymentId(), event.getStatus());

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(record);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                RecordMetadata md = result.getRecordMetadata();
                log.info("[KAFKA ACK] Payment {} written to topic={}, partition={}, offset={}",
                        event.getPaymentId(), md.topic(), md.partition(), md.offset());
            } else {
                log.error("[KAFKA NACK] Failed to publish payment {}: {}", event.getPaymentId(), ex.getMessage(), ex);
            }
        });

        return future;
    }
}
