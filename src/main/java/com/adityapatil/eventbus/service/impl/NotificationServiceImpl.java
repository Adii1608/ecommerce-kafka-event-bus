package com.adityapatil.eventbus.service.impl;

import com.adityapatil.eventbus.model.OrderCreatedEvent;
import com.adityapatil.eventbus.model.PaymentEvent;
import com.adityapatil.eventbus.service.NotificationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Notification dispatching service.
 *
 * SOLID: SRP - Pure notification handling logic (email/SMS simulation).
 */
@Service
public class NotificationServiceImpl implements NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationServiceImpl.class);

    @Override
    public void notifyOrderCreated(OrderCreatedEvent order) {
        log.info("[NOTIFICATION SERVICE] Customer confirmation sent for orderId={} to customerId={}",
                order.getOrderId(), order.getCustomerId());
    }

    @Override
    public void notifyPaymentProcessed(PaymentEvent payment) {
        log.info("[NOTIFICATION SERVICE] Payment receipt sent for orderId={}, paymentId={}, status={}",
                payment.getOrderId(), payment.getPaymentId(), payment.getStatus());
    }
}
