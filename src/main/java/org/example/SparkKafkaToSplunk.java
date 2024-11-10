package org.example;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.entity.StringEntity;
import org.apache.http.client.HttpClient;
import java.util.HashMap;
import java.util.Map;

public class SparkKafkaToSplunk {

    public static void main(String[] args) throws InterruptedException {
        // Initialize SparkConf and Streaming Context
        SparkConf conf = new SparkConf().setAppName("SparkKafkaToSplunk").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(2000)); // 2 seconds batch interval

        // Kafka Consumer Configuration
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "spark-group");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        String topic = "spark_topic"; // Topic where the data is coming from
        JavaDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(java.util.Collections.singletonList(topic), kafkaParams)
        );

        // Processing Kafka Stream Data
        messages.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                // Create a Spark session to process the data
                SparkSession spark = SparkSession.builder()
                        .appName("SparkKafkaToSplunk")
                        .getOrCreate();

                // Process the incoming data (e.g., convert to DataFrame)
                Dataset<Row> df = spark.read().json(rdd.map(ConsumerRecord::value));

                // Example: simple filtering or aggregation (adjust per your use case)
                Dataset<Row> completedTripsDF = df.filter(df.col("trip_status").equalTo("completed"));

                // Send each completed trip event to Splunk for indexing
                completedTripsDF.foreach(row -> {
                    String splunkEvent = row.toString();  // Convert row to string format for Splunk

                    // Send the event to Splunk via HTTP Event Collector
                    sendToSplunk(splunkEvent);
                });
            }
        });

        // Start the streaming job
        jssc.start();
        jssc.awaitTermination();
    }

    // Function to send event to Splunk via HTTP Event Collector
    public static void sendToSplunk(String event) {
        try {
            String splunkUrl = "http://localhost:8088"; // Splunk HTTP Event Collector URL
            String token = "58f41531-07c9-47de-a5af-cb2510d07f0d"; // Replace with your HEC token

            HttpClient client = HttpClients.createDefault();
            HttpPost post = new HttpPost(splunkUrl + "/services/collector/event");
            post.setHeader("Authorization", "Splunk " + token);
            post.setHeader("Content-Type", "application/json");

            String json = "{\"event\": \"" + event + "\", \"index\": \"main\", \"sourcetype\": \"spark_streaming\"}";
            StringEntity entity = new StringEntity(json);
            post.setEntity(entity);

            // Execute the HTTP POST request to Splunk
            client.execute(post);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
