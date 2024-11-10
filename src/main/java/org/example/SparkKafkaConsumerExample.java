package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.functions;
import scala.collection.Seq;

import java.util.HashMap;
import java.util.Map;
import java.util.Collections;

public class SparkKafkaConsumerExample {
    public static void main(String[] args) throws InterruptedException {
        // Get the Kafka Bootstrap server from arguments or environment variable
//        String kafkaBootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        String kafkaBootstrapServers = "localhost:9092";
//        if (kafkaBootstrapServers == null || kafkaBootstrapServers.isEmpty()) {
//            System.err.println("Error: KAFKA_BOOTSTRAP_SERVERS environment variable is not set.");
//            System.exit(1);
//        }

        // Kafka Consumer Configuration
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "spark-group");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Spark Streaming Configuration
        SparkConf sparkConf = new SparkConf().setAppName("SparkKafkaConsumer");

        // Create Java Streaming Context
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, new Duration(2000)); // 2 seconds batch interval

        // Define Kafka topic to consume from (Spark topic)
        String topic = "spark_topic";

        // Create direct stream to consume messages from Kafka
        JavaDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(Collections.singletonList(topic), kafkaParams)
        );

        // Process Kafka messages (Trip Data)
        messages.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
//                 Create a Spark session to process the data
                SparkSession spark = SparkSession.builder()
                        .appName("SparkKafkaConsumer")
                        .getOrCreate();

                // Define the schema for the trip data (from the trip message structure)
                StructType schema = new StructType()
                        .add("driver_id", DataTypes.StringType)
                        .add("passenger_id", DataTypes.StringType)
                        .add("trip_status", DataTypes.StringType)
                        .add("trip_distance", DataTypes.DoubleType)
                        .add("trip_fare", DataTypes.DoubleType)
                        .add("timestamp", DataTypes.LongType);

                // Convert RDD to Dataset directly
                JavaRDD<String> tripDataRDD = rdd.map(ConsumerRecord::value);

                // Create a DataFrame from the RDD of Strings (this step converts JSON strings into structured data)
                Dataset<Row> tripDataDF = spark.read().json(tripDataRDD);

                // Perform analysis on the trip data (e.g., calculate average fare, filter completed trips, etc.)
                Dataset<Row> completedTripsDF = tripDataDF.filter(tripDataDF.col("trip_status").equalTo("completed"));

                // Perform some aggregation on the completed trips (e.g., total fare, average fare, etc.)
                Dataset<Row> fareAnalysisDF = completedTripsDF.groupBy("driver_id")
                        .agg(functions.avg("trip_fare").alias("avg_fare"),
                                functions.sum("trip_fare").alias("total_fare"),
                                functions.avg("trip_distance").alias("avg_distance"));

                // Show the analysis results (Driver-wise fare statistics)
                fareAnalysisDF.show();

                // Optionally, you can store this analysis into a database or a file
                // fareAnalysisDF.write().format("jdbc").option("url", jdbcUrl).option("dbtable", "trip_analysis").save();
            }
        });

        // Start the Spark Streaming job
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
