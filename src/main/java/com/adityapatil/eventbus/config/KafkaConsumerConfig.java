package com.adityapatil.eventbus.config;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * Configures Kafka Consumer listener containers and resilient error handling.
 *
 * Concepts demonstrated:
 * 1. DefaultErrorHandler: Intercepts exceptions thrown during message processing.
 * 2. ExponentialBackOff: Retries failed messages with an increasing delay (e.g. 1s -> 2s -> 4s).
 * 3. DeadLetterPublishingRecoverer: Once retries are exhausted, automatically publishes the poisoned
 *    message and its error stack trace to a Dead Letter Topic (DLT), allowing partition consumption to continue.
 */
@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Value("${ecommerce.topics.orders-dlt:orders.created.DLT}")
    private String ordersDltTopic;

    @Bean
    public CommonErrorHandler errorHandler(KafkaOperations<Object, Object> template) {
        // Recoverer routes failed messages to our explicit DLT topic
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
                (record, exception) -> {
                    log.error("[DLT ROUTER] Routing failed record [key={}, partition={}, offset={}] to topic: {}",
                            record.key(), record.partition(), record.offset(), ordersDltTopic, exception);
                    return new TopicPartition(ordersDltTopic, record.partition());
                });

        // Exponential backoff: 500ms initial interval, multiplier 2.0, max 2500ms total retry time (~2 retries)
        ExponentialBackOff backOff = new ExponentialBackOff(500L, 2.0);
        backOff.setMaxElapsedTime(2500L);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

        // Log each retry attempt
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) ->
                log.warn("[RETRY ATTEMPT #{}] Key='{}', Partition={}, Offset={}. Reason: {}",
                        deliveryAttempt, record.key(), record.partition(), record.offset(), ex.getMessage())
        );

        return errorHandler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConsumerFactory<Object, Object> consumerFactory,
            CommonErrorHandler errorHandler) {

        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);

        // Allows up to 3 concurrent worker threads per listener container matching the 3 partitions
        factory.setConcurrency(3);

        return factory;
    }
}
