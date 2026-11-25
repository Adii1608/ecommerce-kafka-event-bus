package com.example.kafka_practice.service;

import com.example.kafka_practice.model.CreateOrderRequest;
import com.example.kafka_practice.model.OrderResponse;

/**
 * Service contract for Order management.
 *
 * SOLID Principles:
 * - SRP: Handles only order-specific business logic.
 * - DIP: Controller depends on this interface, not the concrete implementation.
 */
public interface OrderService {

    /**
     * Accepts and processes a new order request.
     *
     * @param request The incoming validated order request
     * @return OrderResponse with generated order ID and status
     */
    OrderResponse createOrder(CreateOrderRequest request);
}
