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
 *  How to programmatically create a Topic.
 *  How to configure a delete cleanup policy.
 *
 * This will create a topic named java.created.topic
 * After doing so run from the CLI to check it out:
 *
 kafka-topics.sh \
 --bootstrap-server localhost:9092 \
 --topic java.created.topic \
 --describe
 *
 */
public class CaTopicCreator {


    private static Logger LOGGER = LoggerFactory.getLogger(CaTopicCreator.class.getName());

    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";
        String topic = "java.created.topic";

        // Producer configuration
        Map<String, String> properties = new HashMap<>();

        // messages older than 1 minute can be cleaned up
        properties.put(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(Duration.ofMinutes(1).toMillis()));
        properties.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);

        /*
        segments in kafka can be up to segment.bytes bytes long or segment.ms old before a new segment is started.
        Here we are setting up segments to only be 1 minute long.

        The delete clean up policy deletes segments and not messages. So if there's at least one message that has
        not fallen into the retention policy the entire segment will be maintained.

        Messages that are marked for deletion will still be consumed if they haven't been cleaned up.

         */
        properties.put(TopicConfig.SEGMENT_MS_CONFIG, String.valueOf(Duration.ofMinutes(1).toMillis()));


        NewTopic newTopic = new NewTopic(topic, 1, (short) 1)
                .configs(properties);

        Properties adminProps = new Properties();
        adminProps.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);


        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            adminClient.createTopics(Collections.singleton(newTopic))
                    .values()
                    .entrySet()
                    .forEach(CaTopicCreator::handelNewTopicCreation);
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
