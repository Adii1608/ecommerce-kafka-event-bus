package com.example.kafka_practice.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;

/**
 * HTTP request body payload for creating a new order.
 */
public record CreateOrderRequest(
        @NotBlank(message = "customerId is required")
        String customerId,

        @NotBlank(message = "productName is required")
        String productName,

        @Positive(message = "quantity must be greater than 0")
        int quantity,

        @NotNull(message = "price is required")
        @Positive(message = "price must be greater than 0")
        BigDecimal price,

        boolean simulateFailure
) {}
