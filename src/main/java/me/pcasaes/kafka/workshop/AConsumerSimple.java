package me.pcasaes.kafka.workshop;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * What we'll learn here:
 *  How to set up a simple consumer in a consumer group (a subscription)
 *  How to configure Kafka classes:
 *      {@link org.apache.kafka.clients.producer.ProducerConfig}
 *      {@link org.apache.kafka.clients.consumer.ConsumerConfig}
 *      {@link org.apache.kafka.common.config.TopicConfig}
 */
public class AConsumerSimple {

    private static Logger LOGGER = LoggerFactory.getLogger(AConsumerSimple.class.getName());

    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "first-app";
        String topic = "simple.topic";

        // consumer configuration
        /*
        In Kafka all configuration values must be Strings.
        All Kafka classes are configured using properties defined in ConsumerConfig, TopicConfig and ProducerConfig
         */
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // will read from beginning

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        /*
        Subscribe to the topic. This will add this consumer to the consumer group configured in group.id.
        If the consumer group already exists it will start from where it last was left off.
        Try this by stopping this app, producing some messages and starting up again.

        consumer offset are recorded on a 50 partition topic named __consumer_offsets
         */
        consumer.subscribe(Arrays.asList(topic));

        // poll non stop. Must kill with sigterm
        while (true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                LOGGER.info(String.format("Key: %s,\tValue: %s\t---\tPartition: %d,\tOffset:%d",
                        record.key(),
                        record.value(),
                        record.partition(),
                        record.offset()));
            }
        }
    }
}
