package com.adityapatil.eventbus.controller;

import com.adityapatil.eventbus.model.CreateOrderRequest;
import com.adityapatil.eventbus.model.OrderResponse;
import com.adityapatil.eventbus.service.OrderService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST Controller exposing order management endpoints.
 *
 * SOLID Principles:
 * - SRP: Exclusively handles HTTP request binding, validation, and HTTP response mapping.
 * - DIP: Injected with the OrderService abstraction, not the concrete implementation.
 */
@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public ResponseEntity<OrderResponse> createOrder(@Valid @RequestBody CreateOrderRequest request) {
        OrderResponse response = orderService.createOrder(request);
        return ResponseEntity.accepted().body(response);
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> healthCheck() {
        return ResponseEntity.ok(Map.of("status", "UP", "service", "OrderController"));
    }
}
