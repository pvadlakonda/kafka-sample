package com.kafka.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class OrderTest {

    private Order sampleOrder() {
        return new Order("order-1", 3, 45.67, 1700000000000L, "Electronics");
    }

    @Test
    void constructorSetsAllFields() {
        Order order = sampleOrder();
        assertEquals("order-1", order.getOrderId());
        assertEquals(3, order.getOrderQuantity());
        assertEquals(45.67, order.getPricePerEach());
        assertEquals(1700000000000L, order.getOrderDate());
        assertEquals("Electronics", order.getProductCategory());
    }

    @Test
    void settersUpdateFields() {
        Order order = sampleOrder();
        order.setOrderId("order-2");
        order.setOrderQuantity(5);
        order.setPricePerEach(9.99);
        order.setOrderDate(1700000001000L);
        order.setProductCategory("Books");

        assertEquals("order-2", order.getOrderId());
        assertEquals(5, order.getOrderQuantity());
        assertEquals(9.99, order.getPricePerEach());
        assertEquals(1700000001000L, order.getOrderDate());
        assertEquals("Books", order.getProductCategory());
    }

    @Test
    void equalOrdersAreEqual() {
        Order a = sampleOrder();
        Order b = sampleOrder();
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void differentOrdersAreNotEqual() {
        Order a = sampleOrder();
        Order b = new Order("order-99", 1, 1.0, 0L, "Toys");
        assertNotEquals(a, b);
    }

    @Test
    void toStringContainsKeyFields() {
        Order order = sampleOrder();
        String str = order.toString();
        assertTrue(str.contains("order-1"));
        assertTrue(str.contains("Electronics"));
    }
}