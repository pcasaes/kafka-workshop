package me.pcasaes.kafka.workshop;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 *
 * What we'll learn here:
 *  How to set up a producer with 0 guarantee's.
 *  How consumer groups work with re-balancing.
 *
 * Start up 4 instances of {@link BConsumerPart3}
 * Then start up an instance of this class (it'll stop eventually but you might have to kill it).
 *
 * Then play around with stopping one or more running instance of the consumer. You should see the consunmers
 * re-balancing.
 *
 */
public class BProducerPart3 {

    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";
        String topic = "part3.topic";

        // Producer configuration
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /*
        This producer will not wait for acknowledgement from the cluster.
         */
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        for (int i = 0; i < 10_000; i++) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, String.valueOf(i));

            producer.send(record); //send data asynchronously

            // sleep for 1 second every 64 messages
            if ((i & 63) == 0) {
                try {
                    Thread.sleep(1_000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }


        producer.flush();
        producer.close();

    }
}
