package com.kafka.producer;

import com.kafka.model.Order;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class ProducerTest {

    private final Producer producer = new Producer();

    @Test
    void createRandomOrderHasValidFields() {
        Order order = producer.createRandomOrder();

        assertNotNull(order.getOrderId());
        assertFalse(order.getOrderId().isBlank());
        assertTrue(order.getOrderQuantity() >= 1 && order.getOrderQuantity() <= 10);
        assertTrue(order.getPricePerEach() >= 0.0 && order.getPricePerEach() <= 100.0);
        assertTrue(order.getOrderDate() > 0);
        assertNotNull(order.getProductCategory());
    }

    @Test
    void createRandomOrderUsesKnownCategory() {
        for (int i = 0; i < 20; i++) {
            Order order = producer.createRandomOrder();
            boolean knownCategory = false;
            for (String cat : Producer.CATEGORIES) {
                if (cat.equals(order.getProductCategory())) {
                    knownCategory = true;
                    break;
                }
            }
            assertTrue(knownCategory, "Unexpected category: " + order.getProductCategory());
        }
    }

    @Test
    void buildPropsContainsRequiredKafkaConfig() {
        Properties props = Producer.buildProps();

        assertNotNull(props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("org.apache.kafka.common.serialization.StringSerializer",
            props.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals("org.apache.kafka.common.serialization.StringSerializer",
            props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
    }
}