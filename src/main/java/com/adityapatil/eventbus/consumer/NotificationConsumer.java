package com.adityapatil.eventbus.consumer;

import com.adityapatil.eventbus.model.OrderCreatedEvent;
import com.adityapatil.eventbus.model.PaymentEvent;
import com.adityapatil.eventbus.service.NotificationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka Messaging Adapter for Notifications.
 *
 * SOLID Principles:
 * - SRP: Acts solely as listener adapter for multi-topic notifications.
 * - DIP: Injects NotificationService abstraction.
 */
@Component
public class NotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);

    private final NotificationService notificationService;

    public NotificationConsumer(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @KafkaListener(
            topics = {
                    "${ecommerce.topics.orders-created:orders.created}",
                    "${ecommerce.topics.payments-events:payments.events}"
            },
            groupId = "notification-service-group"
    )
    public void onEventReceived(ConsumerRecord<String, Object> record) {
        log.info("[NOTIFICATION CONSUMER] Received event from topic='{}' partition={} offset={}",
                record.topic(), record.partition(), record.offset());

        Object payload = record.value();
        if (payload instanceof OrderCreatedEvent order) {
            notificationService.notifyOrderCreated(order);
        } else if (payload instanceof PaymentEvent payment) {
            notificationService.notifyPaymentProcessed(payment);
        }
    }
}
