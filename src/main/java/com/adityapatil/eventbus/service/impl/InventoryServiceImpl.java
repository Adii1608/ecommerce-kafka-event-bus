package com.adityapatil.eventbus.service.impl;

import com.adityapatil.eventbus.model.OrderCreatedEvent;
import com.adityapatil.eventbus.service.InventoryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Inventory reservation service.
 *
 * SOLID: SRP - Pure domain logic for stock allocation without knowledge of Kafka annotations.
 */
@Service
public class InventoryServiceImpl implements InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryServiceImpl.class);

    @Override
    public void reserveStock(OrderCreatedEvent order) {
        log.info("[INVENTORY SERVICE] Stock reserved: {} unit(s) of '{}' for orderId={}",
                order.getQuantity(), order.getProductName(), order.getOrderId());
    }
}
