package me.pcasaes.kafka.workshop;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * What we'll learn here:
 *  How to set up a non subscribing consumer.
 *
 */
public class CNonSubscribingConsumer {


    private static Logger LOGGER = LoggerFactory.getLogger(BConsumerPart3.class.getName());

    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";
        String topic = "java.created.topic";

        // consumer configuration
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        /*
        Here we will not be creating a subscription. Instead we will read from the topics/partitions we are
        interested in from whatever offset we'd like. Nothing of this will tracked nor be involved in a
        consumer re-balance.

        A use case for this is maintaining an application's state in memory. Kafka acts as a persistence log.
        You can even use compaction cleanup for snapshotting.

         */
        Collection<TopicPartition> topicPartitions = consumer.partitionsFor(topic)
                .stream()
                .map(pInfo -> new TopicPartition(pInfo.topic(), pInfo.partition()))
                .collect(Collectors.toList());

        consumer.assign(topicPartitions);

        /*
        This will move us to the beginning of the topic. Otherwise we will be at latest.

        Check out the other seek methods.
         */
        consumer.seekToBeginning(topicPartitions);

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
