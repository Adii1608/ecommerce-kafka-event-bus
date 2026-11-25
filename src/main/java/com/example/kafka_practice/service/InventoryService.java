package com.example.kafka_practice.service;

import com.example.kafka_practice.model.OrderCreatedEvent;

/**
 * Service contract for stock and inventory operations.
 */
public interface InventoryService {

    /**
     * Reserves stock for the items in the order.
     *
     * @param order The order event
     */
    void reserveStock(OrderCreatedEvent order);
}
