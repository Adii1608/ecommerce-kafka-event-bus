package com.example.kafka_practice.model;

import java.time.Instant;
import java.util.Objects;

/**
 * Response DTO returned to clients upon order submission.
 * Record + Builder Pattern.
 */
public record OrderResponse(
        String orderId,
        String customerId,
        String status,
        String message,
        Instant submittedAt
) {

    public static Builder builder() {
        return new Builder();
    }

    public String getOrderId() { return orderId; }
    public String getCustomerId() { return customerId; }
    public String getStatus() { return status; }
    public String getMessage() { return message; }
    public Instant getSubmittedAt() { return submittedAt; }

    public static final class Builder {
        private String orderId;
        private String customerId;
        private String status;
        private String message;
        private Instant submittedAt;

        public Builder orderId(String orderId) {
            this.orderId = orderId;
            return this;
        }

        public Builder customerId(String customerId) {
            this.customerId = customerId;
            return this;
        }

        public Builder status(String status) {
            this.status = status;
            return this;
        }

        public Builder message(String message) {
            this.message = message;
            return this;
        }

        public Builder submittedAt(Instant submittedAt) {
            this.submittedAt = submittedAt;
            return this;
        }

        public OrderResponse build() {
            Objects.requireNonNull(orderId, "orderId must not be null");
            Objects.requireNonNull(status, "status must not be null");
            return new OrderResponse(
                    orderId,
                    customerId,
                    status,
                    message,
                    submittedAt != null ? submittedAt : Instant.now()
            );
        }
    }
}
