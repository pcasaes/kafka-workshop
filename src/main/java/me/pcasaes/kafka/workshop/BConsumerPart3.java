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
 * @see BProducerPart3
 */
public class BConsumerPart3 {

    private static Logger LOGGER = LoggerFactory.getLogger(BConsumerPart3.class.getName());

    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "second-app";
        String topic = "part3.topic";

        // consumer configuration
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
