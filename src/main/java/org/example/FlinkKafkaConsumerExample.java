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
        // Start consuming from the latest offsets
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");


        // Create a Kafka consumer
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "flink_topic",                    // Topic name
                new SimpleStringSchema(),        // Deserialization schema
                properties
        );

        // Create the data stream
        DataStream<String> stream = env.addSource(consumer);

        // Process the stream: Extract movie_id, action (view/rate), and rating (if applicable)
        DataStream<Tuple2<String, Integer>> result = stream
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        // Parse the incoming JSON message
                        String movieId = value.split("\"movie_id\": \"")[1].split("\"")[0];
                        String action = value.split("\"action\": \"")[1].split("\"")[0];
                        int rating = action.equals("rate") ? Integer.parseInt(value.split("\"rating\": ")[1].split(",")[0]) : 0;

                        // Emit movie_id and the corresponding view/rating count
                        return new Tuple2<>(movieId, rating);
                    }
                });

        // Aggregate the data: Count views and ratings per movie
        result
                .keyBy(0)  // Group by movie_id
                .sum(1)    // Sum the ratings (can be extended for other metrics like views)
                .print();

        // Execute the job
        env.execute("Flink Kafka Consumer Example");
    }
}
