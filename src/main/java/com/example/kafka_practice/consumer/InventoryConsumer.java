package com.example.kafka_practice.consumer;

import com.example.kafka_practice.model.OrderCreatedEvent;
import com.example.kafka_practice.service.InventoryService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka Messaging Adapter for Inventory.
 *
 * SOLID Principles:
 * - SRP: Bridges Kafka messages to domain business logic.
 * - DIP: Injects InventoryService abstraction.
 */
@Component
public class InventoryConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryConsumer.class);

    private final InventoryService inventoryService;

    public InventoryConsumer(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    @KafkaListener(
            topics = "${ecommerce.topics.orders-created:orders.created}",
            groupId = "inventory-service-group"
    )
    public void onOrderCreated(ConsumerRecord<String, OrderCreatedEvent> record) {
        log.info("[INVENTORY CONSUMER] Received record from partition={} offset={}",
                record.partition(), record.offset());

        inventoryService.reserveStock(record.value());
    }
}
