package com.kafka.producer;

import com.google.gson.Gson;
import com.kafka.config.KafkaConfig;
import com.kafka.model.Order;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class Producer {
    private static final Gson gson = new Gson();
    private static final Random random = new Random();
    private static final String[] CATEGORIES = {"Electronics", "Books", "Toys", "Clothing"};

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("Starting order stream...");
            while (true) {
                Order order = new Order(
                    UUID.randomUUID().toString(),
                    random.nextInt(10) + 1,
                    Math.round(random.nextDouble() * 100 * 100.0) / 100.0,
                    System.currentTimeMillis(),
                    CATEGORIES[random.nextInt(CATEGORIES.length)]
                );
                
                String json = gson.toJson(order);
                producer.send(new ProducerRecord<>(KafkaConfig.TOPIC_NAME, order.getOrderId(), json));
                System.out.printf("Sent Order: %s | %s | Qty: %d | $%.2f%n", 
                    order.getOrderId(), order.getProductCategory(), order.getOrderQuantity(), order.getPricePerEach());
                
                Thread.sleep(1000);
            }
        }
    }
}

