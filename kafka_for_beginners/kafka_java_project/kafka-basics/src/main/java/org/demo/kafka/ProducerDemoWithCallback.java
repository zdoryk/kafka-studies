package org.demo.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am Kafka Producer With Callback!");

        // create Producer Properties
        Properties properties = new Properties();

        // Connect to the Kafka cluster
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

//        properties.setProperty("batch.size", "400");

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        for (int j=0; j<10; j++) {
            for(int i = 0; i < 30; i++) {
                // create a producer record
                ProducerRecord<String, String> record =
                        new ProducerRecord<String, String>("demo_java", "Hello World " + i);

                // send data
                // -- asynchronous
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // executed every time a record is successfully sent or an exception is thrown
                        if (e == null) {
                            // the record was successfully sent
                            log.info("Received new metadata. \n" +
                                    "Topic: " + metadata.topic() + "\n" +
                                    "Partition: " + metadata.partition() + "\n" +
                                    "Offset: " + metadata.offset() + "\n" +
                                    "Timestamp: " + metadata.timestamp());
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });
            }

            try{
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // create a producer record
//        ProducerRecord<String, String> record =
//                new ProducerRecord<String, String>("demo_java", "Hello World");
//
//        // send data
//        // -- asynchronous
//        producer.send(record, new Callback() {
//            @Override
//            public void onCompletion(RecordMetadata metadata, Exception e) {
//                // executed every time a record is successfully sent or an exception is thrown
//                if (e == null) {
//                    // the record was successfully sent
//                    log.info("Received new metadata. \n" +
//                            "Topic: " + metadata.topic() + "\n" +
//                            "Partition: " + metadata.partition() + "\n" +
//                            "Offset: " + metadata.offset() + "\n" +
//                            "Timestamp: " + metadata.timestamp());
//                } else {
//                    log.error("Error while producing", e);
//                }
//            }
//        });

        // flush and close the producer
        // tell the producer to send all data and block until done -- synchronous
        producer.flush();

        // flush and close the producer
        producer.close();
    }
}
