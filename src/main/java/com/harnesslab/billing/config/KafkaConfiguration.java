package com.harnesslab.billing.config;

import com.harnesslab.billing.types.MalformedEventException;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfiguration {

    /** Suffix of the dead-letter topic, for example sim.activated.billing-dlt. */
    public static final String DEAD_LETTER_SUFFIX = ".billing-dlt";

    /**
     * A malformed message goes to the dead-letter topic unchanged, once, so it never blocks the partition.
     * Every other failure (database or Kafka down) is retried every second until it succeeds; no event is skipped.
     */
    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<byte[], byte[]> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, exception) -> new TopicPartition(record.topic() + DEAD_LETTER_SUFFIX, -1));
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, FixedBackOff.UNLIMITED_ATTEMPTS));
        handler.addNotRetryableExceptions(MalformedEventException.class);
        return handler;
    }
}
