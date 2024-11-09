package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class KafkaProducerExample {

    public static void main(String[] args) {
        // Kafka Producer Configuration
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Sample data to send
        String[] messages = {
                "Message 1: Student A viewed course X",
                "Message 2: Student B completed quiz Y",
                "Message 3: Student A started course Z",
                "Message 4: Student C dropped course X",
                "Message 5: Student B viewed course Y"
        };

        // Sending messages to Kafka topics (flink_topic and spark_topic)
        for (String message : messages) {
            // Send to Flink topic
            ProducerRecord<String, String> flinkRecord = new ProducerRecord<>("flink_topic", message);
            // Send to Spark topic
            ProducerRecord<String, String> sparkRecord = new ProducerRecord<>("spark_topic", message);

            try {
                producer.send(flinkRecord);
                producer.send(sparkRecord);
                System.out.println("Sent: " + message);
                Thread.sleep(10); // Delay between messages
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        producer.close();
    }
}
