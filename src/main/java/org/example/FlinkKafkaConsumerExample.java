package org.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class FlinkKafkaConsumerExample {

    public static void main(String[] args) throws Exception {

        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set Kafka properties
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Kafka broker
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-group");


        // Create a Kafka consumer
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "flink_topic",                    // Topic name
                new SimpleStringSchema(),        // Deserialization schema
                properties
        );

        // Create the data stream
        DataStream<String> stream = env.addSource(consumer);

        // Process the stream
        DataStream<Tuple2<String, Integer>> result = stream
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return new Tuple2<>(value, value.length());  // Example processing
                    }
                });

        // Print the result
        result.print();

        // Execute the job
        env.execute("Flink Kafka Consumer Example");
    }
}
