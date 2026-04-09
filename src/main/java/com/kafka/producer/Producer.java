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

    static final String[] CATEGORIES = {"Electronics", "Books", "Toys", "Clothing"};

    private final Gson gson = new Gson();
    private final Random random = new Random();

    Order createRandomOrder() {
        return new Order(
            UUID.randomUUID().toString(),
            random.nextInt(10) + 1,
            Math.round(random.nextDouble() * 100 * 100.0) / 100.0,
            System.currentTimeMillis(),
            CATEGORIES[random.nextInt(CATEGORIES.length)]
        );
    }

    static Properties buildProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    void sendOrder(KafkaProducer<String, String> kafkaProducer, Order order) {
        String json = gson.toJson(order);
        kafkaProducer.send(new ProducerRecord<>(KafkaConfig.TOPIC_NAME, order.getOrderId(), json));
        System.out.printf("Sent Order: %s | %s | Qty: %d | $%.2f%n",
            order.getOrderId(), order.getProductCategory(),
            order.getOrderQuantity(), order.getPricePerEach());
    }

    void run() throws InterruptedException {
        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(buildProps())) {
            System.out.println("Starting order stream...");
            while (true) {
                sendOrder(kafkaProducer, createRandomOrder());
                Thread.sleep(1000);
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        new Producer().run();
    }
}