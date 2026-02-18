package com.kafka.consumer;

import com.google.gson.Gson;
import com.kafka.config.KafkaConfig;
import com.kafka.model.Order;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Consumer {
    private static final Gson gson = new Gson();
    private static final Map<String, Double> categoryTotals = new HashMap<>();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConfig.CONSUMER_GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(KafkaConfig.TOPIC_NAME));
            System.out.println("Streaming orders and calculating totals by category...\n");
            
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(record -> {
                    Order order = gson.fromJson(record.value(), Order.class);
                    double totalPrice = order.getOrderQuantity() * order.getPricePerEach();
                    
                    categoryTotals.merge(order.getProductCategory(), totalPrice, Double::sum);
                    
                    System.out.printf("Order: %s | %s | Qty: %d × $%.2f = $%.2f%n",
                        order.getOrderId(), order.getProductCategory(), order.getOrderQuantity(), order.getPricePerEach(), totalPrice);
                    
                    System.out.println("\n=== CUMULATIVE TOTALS BY CATEGORY ===");
                    categoryTotals.forEach((category, total) -> 
                        System.out.printf("  %s: $%.2f%n", category, total));
                    System.out.println("=====================================\n");
                });
            }
        }
    }
}

