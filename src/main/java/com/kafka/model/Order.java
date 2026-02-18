package com.kafka.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Order {
    private String orderId;
    private int orderQuantity;
    private double pricePerEach;
    private long orderDate;
    private String productCategory;
}
