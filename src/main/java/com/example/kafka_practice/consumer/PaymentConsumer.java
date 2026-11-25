package com.example.kafka_practice.consumer;

import com.example.kafka_practice.model.OrderCreatedEvent;
import com.example.kafka_practice.service.PaymentProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka Messaging Adapter for Payments.
 *
 * SOLID Principles:
 * - SRP: Acts solely as the messaging bridge (KafkaListener transport).
 * - DIP: Delegates domain processing entirely to the PaymentProcessor abstraction.
 */
@Component
public class PaymentConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentConsumer.class);

    private final PaymentProcessor paymentProcessor;

    public PaymentConsumer(PaymentProcessor paymentProcessor) {
        this.paymentProcessor = paymentProcessor;
    }

    @KafkaListener(
            topics = "${ecommerce.topics.orders-created:orders.created}",
            groupId = "payment-service-group"
    )
    public void onOrderCreated(ConsumerRecord<String, OrderCreatedEvent> record) {
        log.info("[PAYMENT CONSUMER] Received record from partition={} offset={}",
                record.partition(), record.offset());

        paymentProcessor.processPayment(record.value());
    }
}
