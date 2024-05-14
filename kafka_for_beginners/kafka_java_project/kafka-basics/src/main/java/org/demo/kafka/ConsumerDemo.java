package org.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am Kafka Consumer!");

        String groupId = "my-java-application";
        String topic = "demo_java";

        // create Consumer Properties
        Properties properties = new Properties();

        // Connect to the Kafka cluster
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // set consumer properties
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());


        properties.setProperty("group.id", groupId);

        properties.setProperty("auto.offset.reset", "earliest"); // earliest/latest/none

        // create the Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscrive to a topic
        consumer.subscribe(Arrays.asList(topic));


        // poll for new data
        while (true) {
            log.info("Polling for new data");

            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));


            records.forEach(record -> {
                log.info("Key: " + record.key() + " | Value: " + record.value());
                log.info("Partition: " + record.partition() + " | Offset: " + record.offset());
            });
        }
    }
}
