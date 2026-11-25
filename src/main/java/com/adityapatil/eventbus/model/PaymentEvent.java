package com.adityapatil.eventbus.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

/**
 * Domain Event representing the outcome of a payment attempt.
 * Record + Builder Pattern.
 */
public record PaymentEvent(
        String paymentId,
        String orderId,
        String status, // SUCCESS or FAILED
        BigDecimal amount,
        Instant timestamp
) {

    public static Builder builder() {
        return new Builder();
    }

    public String getPaymentId() { return paymentId; }
    public String getOrderId() { return orderId; }
    public String getStatus() { return status; }
    public BigDecimal getAmount() { return amount; }
    public Instant getTimestamp() { return timestamp; }

    public static final class Builder {
        private String paymentId;
        private String orderId;
        private String status;
        private BigDecimal amount;
        private Instant timestamp;

        public Builder paymentId(String paymentId) {
            this.paymentId = paymentId;
            return this;
        }

        public Builder orderId(String orderId) {
            this.orderId = orderId;
            return this;
        }

        public Builder status(String status) {
            this.status = status;
            return this;
        }

        public Builder amount(BigDecimal amount) {
            this.amount = amount;
            return this;
        }

        public Builder timestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public PaymentEvent build() {
            Objects.requireNonNull(paymentId, "paymentId must not be null");
            Objects.requireNonNull(orderId, "orderId must not be null");
            Objects.requireNonNull(status, "status must not be null");
            Objects.requireNonNull(amount, "amount must not be null");
            return new PaymentEvent(
                    paymentId,
                    orderId,
                    status,
                    amount,
                    timestamp != null ? timestamp : Instant.now()
            );
        }
    }
}
