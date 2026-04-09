package com.kafka.producer;

import com.google.gson.Gson;
import com.kafka.model.Order;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class OrderSerializationTest {

    private static final Gson gson = new Gson();

    @Test
    void orderSerializesToJsonWithCorrectFields() {
        Order order = new Order("abc-123", 2, 19.99, 1700000000000L, "Books");
        String json = gson.toJson(order);

        assertTrue(json.contains("\"orderId\":\"abc-123\""));
        assertTrue(json.contains("\"orderQuantity\":2"));
        assertTrue(json.contains("\"pricePerEach\":19.99"));
        assertTrue(json.contains("\"productCategory\":\"Books\""));
        assertTrue(json.contains("\"orderDate\":1700000000000"));
    }

    @Test
    void orderDeserializesFromJson() {
        String json = "{\"orderId\":\"abc-123\",\"orderQuantity\":2,\"pricePerEach\":19.99," +
                      "\"orderDate\":1700000000000,\"productCategory\":\"Books\"}";

        Order order = gson.fromJson(json, Order.class);

        assertEquals("abc-123", order.getOrderId());
        assertEquals(2, order.getOrderQuantity());
        assertEquals(19.99, order.getPricePerEach());
        assertEquals(1700000000000L, order.getOrderDate());
        assertEquals("Books", order.getProductCategory());
    }

    @Test
    void roundTripPreservesAllFields() {
        Order original = new Order("xyz-789", 5, 7.50, 1700000005000L, "Toys");
        Order restored = gson.fromJson(gson.toJson(original), Order.class);
        assertEquals(original, restored);
    }
}