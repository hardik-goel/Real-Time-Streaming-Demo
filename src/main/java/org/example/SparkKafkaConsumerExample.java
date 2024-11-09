package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import java.util.HashMap;
import java.util.Map;

public class SparkKafkaConsumerExample {

    public static void main(String[] args) throws InterruptedException {
        // Spark Streaming Configuration
        SparkConf sparkConf = new SparkConf().setAppName("SparkKafkaConsumer").setMaster("local[2]");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, new Duration(2000)); // 2 seconds batch interval

        // Kafka Consumer Configuration
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "spark-group");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Define the Kafka topic to consume from (Spark topic)
        String topic = "spark_topic";

        // Create direct stream to consume messages from Kafka
        JavaDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(java.util.Collections.singletonList(topic), kafkaParams)
        );

        // Process the Kafka messages
        messages.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                System.out.println("Received Messages:");
                rdd.foreach(record -> System.out.println(record.value()));
            }
        });

        // Start the Spark Streaming job
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
