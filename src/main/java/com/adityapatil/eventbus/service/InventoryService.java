package com.adityapatil.eventbus.service;

import com.adityapatil.eventbus.model.OrderCreatedEvent;

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
