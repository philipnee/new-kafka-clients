package org.pnee.kafka.clients.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * MY implementation of the KafkaConsumer that uses CompletableFuture and builder patterns to eliminate several usability issues. The general pattern of using this consumer goes like this:
 * <p>
 * <pre>
 *     PafkaConsumer<String, String> consumer = new PafkaConsumer.Builder()
 *         .setBootstrapServer("localhost:9021")
 *         .setKeyDeserializer("org.apache.kafka.common.serialization.StringDeserializer")
 *         .setValDeserializer("org.apache.kafka.common.serialization.StringDeserializer")
 *         .build();
 *
 *     // autoPoll with autocommit
 *     Stream<ConsumerRecord<String, String>> results = consumer
 *         .withAssignment(topicPartitionAssignments)
 *         .withTimeout(duration)
 *         .withCommitStrategy(CommitStrategy.AUTO)
 *         .autoPoll();
 *
 *     // autoPoll with Manual commit
 *     Stream<ConsumerRecord<String, String>> res2 = consumer
 *         .withAssignment(topicPartitionAssignments)
 *         .withTimeout(duration)
 *         .withCommitStrategy(CommitStrategy.ON_POLL, 2)
 *         .autoPoll();
 * </pre>
 * </p>
 * @param <K>
 * @param <V>
 */
public class PafkaConsumer<K, V> {

    private final Time time;
    private DefaultPafkaExceptionHandler handler;

    public PafkaConsumer(final Map<String, Object> configs) {
        this.time = Time.SYSTEM;

    }

    public PafkaConsumer withAssignment(final Collection<TopicPartition> partitions) {
        return null;
    }

    public PafkaConsumer withSubscription(final Collection<String> topics) {
        return null;
    }

    public PafkaConsumer withSubscription(final Pattern pattern) {
        return null;
    }

    public PafkaConsumer withExceptionHandler(final PafkaExceptionHandler handler) {
        if (handler == null) {
            this.handler = new DefaultPafkaExceptionHandler();
        }
        return null;
    }

    public Future<ConsumerRecord<K, V>> pollOnce() {
        return null;
    }

    public Stream<ConsumerRecord<K, V>> autoPoll() {
        return null;
    }

    public PafkaConsumer withCommitStrategy(CommitStrategy commitStrategy) {
        return null;
    }

    public void close() {
    }

    public static enum CommitStrategy {
        ON_POLL,
        ON_TIME,
        AUTO,
    }

    public static class Builder {
        private String bootstrapServer;
        private String keyDeserializer;
        private String valDeserializer;
        private Map<String, Object> configs;
        private ConsumerRebalanceListener listener;
        private OffsetCommitCallback commitCallback;

        public Builder(final Map<String, Object> optionalConfig) {
            configs.putAll(optionalConfig);
        }

        public Builder setBootstrapServer(final String bootstrapServer) {
            this.bootstrapServer = bootstrapServer;
            return this;
        }

        public Builder setKeyDeserializer(final String keyDeserializer) {
            this.keyDeserializer = keyDeserializer;
            return this;
        }

        public Builder setValDeserializer(final String valDeserializer) {
            this.valDeserializer = valDeserializer;
            return this;
        }
        
        public Builder setRebalanceCallback(final ConsumerRebalanceListener listener) {
            this.listener = listener;
            return this;
        }
        
        public Builder setCommitCallback(final OffsetCommitCallback commitCallback) {
            this.commitCallback = commitCallback;
            return this;
        }

        public PafkaConsumer build() {
            return new PafkaConsumer(configs);
        }
    }

    private class DefaultPafkaExceptionHandler {
        public void handle(Exception exception) {
            // just trolling
            throw new RuntimeException(exception);
        }
    }
}
