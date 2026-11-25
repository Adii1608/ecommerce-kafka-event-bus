package com.example.kafka_practice;

import com.example.kafka_practice.model.OrderCreatedEvent;
import com.example.kafka_practice.publisher.EventPublisher;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(
        partitions = 3,
        topics = {"orders.created", "payments.events", "inventory.events", "orders.created.DLT"}
)
class OrderFlowIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    // DIP: Injects the interface abstraction EventPublisher<OrderCreatedEvent>
    @Autowired
    private EventPublisher<OrderCreatedEvent> orderEventPublisher;

    private Consumer<String, Object> testConsumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
                "test-verifier-group-" + UUID.randomUUID(),
                "true",
                embeddedKafkaBroker
        );
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        consumerProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");

        DefaultKafkaConsumerFactory<String, Object> consumerFactory =
                new DefaultKafkaConsumerFactory<>(consumerProps);
        testConsumer = consumerFactory.createConsumer();
    }

    @AfterEach
    void tearDown() {
        if (testConsumer != null) {
            testConsumer.close();
        }
    }

    @Test
    @DisplayName("Happy path: order is constructed via Builder, published, and payment event is generated")
    void testHappyPathOrderLifecycle() {
        testConsumer.subscribe(Collections.singletonList("payments.events"));
        testConsumer.poll(Duration.ofMillis(500)); // Ensure partition assignment

        String orderId = "ORD-TEST-1";

        // Builder Pattern in action
        OrderCreatedEvent event = OrderCreatedEvent.builder()
                .orderId(orderId)
                .customerId("CUST-100")
                .productName("Laptop")
                .quantity(1)
                .price(new BigDecimal("1200.00"))
                .simulateFailure(false)
                .createdAt(Instant.now())
                .build();

        orderEventPublisher.publish(event.getCustomerId(), event);

        // Verify that PaymentConsumer processed the order and published a PaymentEvent to payments.events
        ConsumerRecords<String, Object> records = KafkaTestUtils.getRecords(testConsumer, Duration.ofSeconds(12));
        assertThat(records.count()).isGreaterThanOrEqualTo(1);

        ConsumerRecord<String, Object> received = records.iterator().next();
        assertThat(received.key()).isEqualTo(orderId);
    }

    @Test
    @DisplayName("Failure path: when payment fails, retries occur and message is routed to DLT")
    void testFailurePathRoutesToDlt() {
        testConsumer.subscribe(Collections.singletonList("orders.created.DLT"));
        testConsumer.poll(Duration.ofMillis(500)); // Ensure partition assignment

        String orderId = "ORD-FAIL-99";

        // Builder Pattern in action
        OrderCreatedEvent failureEvent = OrderCreatedEvent.builder()
                .orderId(orderId)
                .customerId("CUST-BAD-GATEWAY")
                .productName("Faulty Item")
                .quantity(1)
                .price(new BigDecimal("50.00"))
                .simulateFailure(true)
                .createdAt(Instant.now())
                .build();

        orderEventPublisher.publish(failureEvent.getCustomerId(), failureEvent);

        // Verify the message arrives in the DLT after retries
        ConsumerRecords<String, Object> dltRecords = KafkaTestUtils.getRecords(testConsumer, Duration.ofSeconds(15));
        assertThat(dltRecords.count()).isGreaterThanOrEqualTo(1);

        ConsumerRecord<String, Object> dltRecord = dltRecords.iterator().next();
        assertThat(dltRecord.key()).isEqualTo("CUST-BAD-GATEWAY");
    }
}
