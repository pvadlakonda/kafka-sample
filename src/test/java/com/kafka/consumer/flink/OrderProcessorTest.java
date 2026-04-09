package com.kafka.consumer.flink;

import com.kafka.model.Order;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class OrderProcessorTest {

    private OrderProcessor processor;
    private Collector<Order> collector;
    private ByteArrayOutputStream stdout;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        processor = new OrderProcessor();
        collector = mock(Collector.class);
        stdout = new ByteArrayOutputStream();
        System.setOut(new PrintStream(stdout));
    }

    @Test
    void singleOrderIsForwardedToCollector() throws Exception {
        Order order = new Order("o1", 2, 10.0, 0L, "Electronics");
        processor.processElement(order, null, collector);
        verify(collector).collect(order);
    }

    @Test
    void singleOrderOutputShowsCorrectTotal() throws Exception {
        Order order = new Order("o1", 3, 5.0, 0L, "Toys");
        processor.processElement(order, null, collector);

        String out = stdout.toString();
        assertTrue(out.contains("Toys"));
        assertTrue(out.contains("$15.00"));
    }

    @Test
    void sameCategoryAccumulatesAcrossMultipleOrders() throws Exception {
        Order first  = new Order("o1", 2, 10.0, 0L, "Books");
        Order second = new Order("o2", 3, 10.0, 0L, "Books");

        processor.processElement(first,  null, collector);
        stdout.reset();
        processor.processElement(second, null, collector);

        String out = stdout.toString();
        assertTrue(out.contains("$50.00"), "Expected cumulative Books total of $50.00 after two orders");
    }

    @Test
    void differentCategoriesAreTrackedSeparately() throws Exception {
        Order electronics = new Order("o1", 1, 100.0, 0L, "Electronics");
        Order clothing    = new Order("o2", 2,  25.0, 0L, "Clothing");

        processor.processElement(electronics, null, collector);
        stdout.reset();
        processor.processElement(clothing, null, collector);

        String out = stdout.toString();
        assertTrue(out.contains("Electronics"), "Electronics category should still appear");
        assertTrue(out.contains("Clothing"),    "Clothing category should appear");
        assertTrue(out.contains("$100.00"),     "Electronics total should be $100.00");
        assertTrue(out.contains("$50.00"),      "Clothing total should be $50.00");
    }

    @Test
    void eachOrderIsCollectedExactlyOnce() throws Exception {
        Order o1 = new Order("o1", 1, 5.0, 0L, "Electronics");
        Order o2 = new Order("o2", 1, 5.0, 0L, "Books");

        processor.processElement(o1, null, collector);
        processor.processElement(o2, null, collector);

        verify(collector, times(1)).collect(o1);
        verify(collector, times(1)).collect(o2);
    }
}