package com.example.kafka_practice.service.impl;

import com.example.kafka_practice.model.OrderCreatedEvent;
import com.example.kafka_practice.model.PaymentEvent;
import com.example.kafka_practice.publisher.EventPublisher;
import com.example.kafka_practice.service.PaymentProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

/**
 * Payment processing service.
 *
 * SOLID Principles:
 * - SRP: Isolated to financial transaction logic.
 * - DIP: Injected with EventPublisher<PaymentEvent> abstraction.
 * - Builder Pattern: Creates PaymentEvent using PaymentEvent.builder().
 */
@Service
public class PaymentServiceImpl implements PaymentProcessor {

    private static final Logger log = LoggerFactory.getLogger(PaymentServiceImpl.class);

    private final EventPublisher<PaymentEvent> paymentEventPublisher;

    public PaymentServiceImpl(EventPublisher<PaymentEvent> paymentEventPublisher) {
        this.paymentEventPublisher = paymentEventPublisher;
    }

    @Override
    public PaymentEvent processPayment(OrderCreatedEvent order) {
        log.info("[PAYMENT SERVICE] Processing payment for orderId={}, amount={}",
                order.getOrderId(), order.getPrice());

        if (order.isSimulateFailure()) {
            log.error("[PAYMENT SERVICE FAILURE] Simulated Gateway Failure for orderId={}", order.getOrderId());
            throw new IllegalStateException("Payment Gateway unreachable for order: " + order.getOrderId());
        }

        // Builder Pattern in action
        PaymentEvent paymentEvent = PaymentEvent.builder()
                .paymentId("PAY-" + UUID.randomUUID().toString().substring(0, 8))
                .orderId(order.getOrderId())
                .status("SUCCESS")
                .amount(order.getPrice())
                .timestamp(Instant.now())
                .build();

        log.info("[PAYMENT SERVICE SUCCESS] Payment authorized: paymentId={}", paymentEvent.getPaymentId());

        paymentEventPublisher.publish(order.getOrderId(), paymentEvent);

        return paymentEvent;
    }
}
