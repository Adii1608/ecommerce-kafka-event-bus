package com.adityapatil.eventbus.service;

import com.adityapatil.eventbus.model.OrderCreatedEvent;
import com.adityapatil.eventbus.model.PaymentEvent;

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
