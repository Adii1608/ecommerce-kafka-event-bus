package com.example.kafka_practice.service;

import com.example.kafka_practice.model.OrderCreatedEvent;
import com.example.kafka_practice.model.PaymentEvent;

/**
 * Service contract for sending customer notifications.
 */
public interface NotificationService {

    /**
     * Sends customer notification when an order is created.
     */
    void notifyOrderCreated(OrderCreatedEvent order);

    /**
     * Sends customer notification when a payment is processed.
     */
    void notifyPaymentProcessed(PaymentEvent payment);
}
