package com.example.kafka_practice.publisher;

import java.util.concurrent.CompletableFuture;

/**
 * Generic abstraction for publishing domain events.
 *
 * SOLID Principles:
 * - DIP (Dependency Inversion Principle): Core business services depend on this abstraction,
 *   not directly on KafkaTemplate or Apache Kafka classes.
 * - ISP (Interface Segregation Principle): Minimal, focused contract containing only what is needed.
 * - LSP (Liskov Substitution Principle): Can be swapped with an InMemoryPublisher, RabbitMQPublisher,
 *   or MockPublisher without affecting any business logic.
 *
 * @param <T> The event payload type
 */
public interface EventPublisher<T> {

    /**
     * Publishes an event with a partition key asynchronously.
     *
     * @param key Partition key for ordering guarantees
     * @param event The event payload
     * @return CompletableFuture representing publication status
     */
    CompletableFuture<?> publish(String key, T event);
}
