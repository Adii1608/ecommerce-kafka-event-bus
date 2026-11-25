package com.example.kafka_practice.service.impl;

import com.example.kafka_practice.model.CreateOrderRequest;
import com.example.kafka_practice.model.OrderCreatedEvent;
import com.example.kafka_practice.model.OrderResponse;
import com.example.kafka_practice.publisher.EventPublisher;
import com.example.kafka_practice.service.OrderService;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

/**
 * OrderService implementation orchestrating order placement.
 *
 * SOLID Principles:
 * - SRP: Responsible solely for creating order entities and triggering their publication.
 * - DIP: Injected with EventPublisher<OrderCreatedEvent> rather than KafkaTemplate.
 * - Builder Pattern: Uses OrderCreatedEvent.builder() and OrderResponse.builder() for readable,
 *   validated construction.
 */
@Service
public class OrderServiceImpl implements OrderService {

    private final EventPublisher<OrderCreatedEvent> orderEventPublisher;

    public OrderServiceImpl(EventPublisher<OrderCreatedEvent> orderEventPublisher) {
        this.orderEventPublisher = orderEventPublisher;
    }

    @Override
    public OrderResponse createOrder(CreateOrderRequest request) {
        String generatedOrderId = "ORD-" + UUID.randomUUID().toString().substring(0, 8);

        // Builder Pattern in action: fluent, clean construction with validation
        OrderCreatedEvent event = OrderCreatedEvent.builder()
                .orderId(generatedOrderId)
                .customerId(request.customerId())
                .productName(request.productName())
                .quantity(request.quantity())
                .price(request.price())
                .simulateFailure(request.simulateFailure())
                .createdAt(Instant.now())
                .build();

        // Publish using customerId as the partition key (DIP in action)
        orderEventPublisher.publish(event.getCustomerId(), event);

        // Builder Pattern for API Response
        return OrderResponse.builder()
                .orderId(generatedOrderId)
                .customerId(request.customerId())
                .status("ORDER_SUBMITTED")
                .message("Order created and dispatched to Kafka topic 'orders.created'")
                .submittedAt(Instant.now())
                .build();
    }
}
