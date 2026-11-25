package com.example.kafka_practice.model;

import java.math.BigDecimal;

public class OrderNotification {
    private String customerName;
    private long orderId;
    private String productName;
    BigDecimal productCost;

    public String getCustomerName() {
        return customerName;
    }

    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }

    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public BigDecimal getProductCost() {
        return productCost;
    }

    public void setProductCost(BigDecimal productCost) {
        this.productCost = productCost;
    }

    @Override
    public String toString() {
        return "OrderNotification{" +
                "customerName='" + customerName + '\'' +
                ", orderId=" + orderId +
                ", productName='" + productName + '\'' +
                ", productCost=" + productCost +
                '}';
    }
}
