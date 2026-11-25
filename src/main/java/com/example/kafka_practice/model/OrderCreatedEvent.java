package com.example.kafka_practice.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

/**
 * Domain Event representing an order placed by a customer.
 * Uses a Java 21 Record for immutability and seamless serialization,
 * combined with the Builder Pattern for validated, fluent object creation.
 */
public record OrderCreatedEvent(
        String orderId,
        String customerId,
        String productName,
        int quantity,
        BigDecimal price,
        boolean simulateFailure,
        Instant createdAt
) {

    public static Builder builder() {
        return new Builder();
    }

    // Java Bean getter aliases for compatibility
    public String getOrderId() { return orderId; }
    public String getCustomerId() { return customerId; }
    public String getProductName() { return productName; }
    public int getQuantity() { return quantity; }
    public BigDecimal getPrice() { return price; }
    public boolean isSimulateFailure() { return simulateFailure; }
    public Instant getCreatedAt() { return createdAt; }

    /**
     * Fluent Builder Pattern for OrderCreatedEvent.
     */
    public static final class Builder {
        private String orderId;
        private String customerId;
        private String productName;
        private int quantity;
        private BigDecimal price;
        private boolean simulateFailure;
        private Instant createdAt;

        public Builder orderId(String orderId) {
            this.orderId = orderId;
            return this;
        }

        public Builder customerId(String customerId) {
            this.customerId = customerId;
            return this;
        }

        public Builder productName(String productName) {
            this.productName = productName;
            return this;
        }

        public Builder quantity(int quantity) {
            this.quantity = quantity;
            return this;
        }

        public Builder price(BigDecimal price) {
            this.price = price;
            return this;
        }

        public Builder simulateFailure(boolean simulateFailure) {
            this.simulateFailure = simulateFailure;
            return this;
        }

        public Builder createdAt(Instant createdAt) {
            this.createdAt = createdAt;
            return this;
        }

        public OrderCreatedEvent build() {
            Objects.requireNonNull(orderId, "orderId must not be null");
            Objects.requireNonNull(customerId, "customerId must not be null");
            Objects.requireNonNull(productName, "productName must not be null");
            Objects.requireNonNull(price, "price must not be null");
            if (quantity <= 0) {
                throw new IllegalArgumentException("quantity must be greater than 0");
            }
            return new OrderCreatedEvent(
                    orderId,
                    customerId,
                    productName,
                    quantity,
                    price,
                    simulateFailure,
                    createdAt != null ? createdAt : Instant.now()
            );
        }
    }
}
