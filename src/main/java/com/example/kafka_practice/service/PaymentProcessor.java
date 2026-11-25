package com.example.kafka_practice.service;

import com.example.kafka_practice.model.OrderCreatedEvent;
import com.example.kafka_practice.model.PaymentEvent;

/**
 * Service contract for processing payments.
 *
 * SOLID Principles:
 * - SRP: Isolated responsibility for financial transaction processing.
 * - ISP: Exposes only payment processing contract.
 */
public interface PaymentProcessor {

    /**
     * Processes payment for an order.
     *
     * @param order The order event
     * @return Resulting PaymentEvent
     */
    PaymentEvent processPayment(OrderCreatedEvent order);
}
