package com.adityapatil.eventbus.service;

import com.adityapatil.eventbus.model.OrderCreatedEvent;
import com.adityapatil.eventbus.model.PaymentEvent;

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
