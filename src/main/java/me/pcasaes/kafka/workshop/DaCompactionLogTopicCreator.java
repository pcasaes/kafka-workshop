package me.pcasaes.kafka.workshop;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * What we'll learn here:
 *  How to set up a topic with compact cleanup policy.
 *  What tombstone records are.
 *
 *
 * Run this to create this topic. Then run {@link DbScoreProducer} followed by {@link DcScoreConsumer}
 * Keep an eye on the consumer's log.
 *
 */
public class DaCompactionLogTopicCreator {


    private static Logger LOGGER = LoggerFactory.getLogger(DaCompactionLogTopicCreator.class.getName());

    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";
        String topic = "scoreboard.topic";

        // Producer configuration
        Map<String, String> properties = new HashMap<>();

        // Will compact the log by the keys.
        properties.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);

        // messages with less then 10 seconds of life will not be compacted
        properties.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, String.valueOf(Duration.ofSeconds(10).toMillis()));

        // tombstones (records without values) will not be removed for at least 1 minute.
        // compaction will remove tombstones from the log
        properties.put(TopicConfig.DELETE_RETENTION_MS_CONFIG, String.valueOf(Duration.ofMinutes(1).toMillis()));

        // If more than 25% of the log is uncompacted will run the cleanup.
        properties.put(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.25");


        // segments will be 1MB big. By default they are 1GB
        properties.put(TopicConfig.SEGMENT_BYTES_CONFIG, String.valueOf(1024 * 1024));


        NewTopic newTopic = new NewTopic(topic, 1, (short) 1)
                .configs(properties);

        Properties adminProps = new Properties();
        adminProps.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);


        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            adminClient.createTopics(Collections.singleton(newTopic))
                    .values()
                    .entrySet()
                    .forEach(DaCompactionLogTopicCreator::handelNewTopicCreation);
        }
    }

    private static void handelNewTopicCreation(Map.Entry<String, KafkaFuture<Void>> entry) {
        final KafkaFuture<Void> future = entry.getValue();
        final String topic = entry.getKey();

        try {
            future.get(10, TimeUnit.SECONDS);
            LOGGER.info("Topic " + topic + " created");

        } catch (InterruptedException ex) {
            LOGGER.warn(ex.getMessage());
            Thread.currentThread().interrupt();
        } catch (TimeoutException ex) {
            LOGGER.error("Could not create topic " + topic, ex);
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof TopicExistsException) {
                LOGGER.info("Topic " + topic + " already exists: " + ex.getMessage());
            } else {
                LOGGER.error("Could not create topic " + topic, ex);
            }
        }
    }
}
