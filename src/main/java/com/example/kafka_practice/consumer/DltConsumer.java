package com.example.kafka_practice.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

/**
 * Dead Letter Topic (DLT) Monitoring Consumer.
 *
 * Consumer Group: 'dlt-monitoring-group'
 * Listens to: 'orders.created.DLT'
 *
 * When an event fails all retry attempts in PaymentConsumer, Spring Kafka's
 * DeadLetterPublishingRecoverer forwards the original record to this topic along with
 * headers detailing the failure reason and original partition/offset.
 */
@Service
public class DltConsumer {

    private static final Logger log = LoggerFactory.getLogger(DltConsumer.class);

    @KafkaListener(
            topics = "${ecommerce.topics.orders-dlt:orders.created.DLT}",
            groupId = "dlt-monitoring-group"
    )
    public void monitorDlt(ConsumerRecord<String, Object> record) {
        log.error("==================== [DEAD LETTER TOPIC ALERT] ====================");
        log.error("Poison/Failed message arrived on DLT topic='{}', partition={}, offset={}, key='{}'",
                record.topic(), record.partition(), record.offset(), record.key());
        log.error("Payload: {}", record.value());

        // Extract metadata headers attached by DeadLetterPublishingRecoverer
        for (Header header : record.headers()) {
            String key = header.key();
            if (key.startsWith("kafka_dlt-")) {
                String val = new String(header.value(), StandardCharsets.UTF_8);
                log.error("  Header -> {} : {}", key, val);
            }
        }
        log.error("===================================================================");
    }
}
