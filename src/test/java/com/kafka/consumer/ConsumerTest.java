package com.kafka.consumer;

import com.google.gson.Gson;
import com.kafka.model.Order;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class ConsumerTest {

    private Consumer consumer;
    private ByteArrayOutputStream stdout;
    private final Gson gson = new Gson();

    @BeforeEach
    void setUp() {
        consumer = new Consumer();
        stdout = new ByteArrayOutputStream();
        System.setOut(new PrintStream(stdout));
    }

    @Test
    void buildPropsContainsRequiredKafkaConfig() {
        Properties props = Consumer.buildProps();

        assertNotNull(props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertNotNull(props.get(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals("org.apache.kafka.common.serialization.StringDeserializer",
            props.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        assertEquals("org.apache.kafka.common.serialization.StringDeserializer",
            props.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
        assertEquals("earliest", props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    }

    @Test
    void processRecordUpdatesCategoryTotal() {
        String json = gson.toJson(new Order("o1", 2, 10.0, 0L, "Electronics"));
        consumer.processRecord(json);
        assertEquals(20.0, consumer.getCategoryTotals().get("Electronics"));
    }

    @Test
    void processRecordAccumulatesSameCategory() {
        consumer.processRecord(gson.toJson(new Order("o1", 2, 10.0, 0L, "Books")));
        consumer.processRecord(gson.toJson(new Order("o2", 3, 10.0, 0L, "Books")));
        assertEquals(50.0, consumer.getCategoryTotals().get("Books"));
    }

    @Test
    void processRecordTracksMultipleCategories() {
        consumer.processRecord(gson.toJson(new Order("o1", 1, 100.0, 0L, "Electronics")));
        consumer.processRecord(gson.toJson(new Order("o2", 2, 25.0, 0L, "Clothing")));

        assertEquals(100.0, consumer.getCategoryTotals().get("Electronics"));
        assertEquals(50.0, consumer.getCategoryTotals().get("Clothing"));
    }

    @Test
    void processRecordOutputsFormattedLine() {
        String json = gson.toJson(new Order("o1", 3, 5.0, 0L, "Toys"));
        consumer.processRecord(json);

        String out = stdout.toString();
        assertTrue(out.contains("Toys"));
        assertTrue(out.contains("$15.00"));
    }
}