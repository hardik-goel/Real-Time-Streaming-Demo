package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;
import java.util.Random;

public class KafkaProducerExample {
    public static void main(String[] args) {
        // Kafka Producer Configuration
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Movie Data for Flink Topic
        String[] movieIds = {"movie_123", "movie_124", "movie_125", "movie_126", "movie_127"};
        String[] userIds = {"user_001", "user_002", "user_003", "user_004", "user_005"};
        String[] actions = {"view", "rate"};

        // Trip Data for Spark Topic
        String[] driverIds = {"driver_001", "driver_002", "driver_003", "driver_004", "driver_005"};
        String[] passengerIds = {"passenger_001", "passenger_002", "passenger_003", "passenger_004", "passenger_005"};
        String[] tripStatuses = {"completed", "cancelled", "in_progress"};

        Random rand = new Random();
        long startTime = System.currentTimeMillis();

        // Generate 10MB of dummy data
        for (int i = 0; i < 1000000; i++) {  // Adjust the iteration count to generate ~10MB worth of data
            // Movie data generation for Flink topic
            String movieId = movieIds[rand.nextInt(movieIds.length)];
            String userId = userIds[rand.nextInt(userIds.length)];
            String action = actions[rand.nextInt(actions.length)];
            int rating = action.equals("rate") ? rand.nextInt(5) + 1 : 0; // Ratings only for 'rate' action
            long timestamp = startTime + rand.nextInt(1000);  // Random timestamps
            String movieMessage = String.format(
                    "{\"movie_id\": \"%s\", \"user_id\": \"%s\", \"action\": \"%s\", \"rating\": %d, \"timestamp\": %d}",
                    movieId, userId, action, rating, timestamp);

            // Trip data generation for Spark topic
            String driverId = driverIds[rand.nextInt(driverIds.length)];
            String passengerId = passengerIds[rand.nextInt(passengerIds.length)];
            String tripStatus = tripStatuses[rand.nextInt(tripStatuses.length)];
            double tripDistance = rand.nextDouble() * 100; // Random distance in km
            double tripFare = tripDistance * (rand.nextDouble() * 2 + 5); // Random fare between $5 and $15 per km
            long tripStartTime = startTime + rand.nextInt(1000);
            String tripMessage = String.format(
                    "{\"driver_id\": \"%s\", \"passenger_id\": \"%s\", \"trip_status\": \"%s\", \"trip_distance\": %.2f, \"trip_fare\": %.2f, \"timestamp\": %d}",
                    driverId, passengerId, tripStatus, tripDistance, tripFare, tripStartTime);

            // Send messages to respective Kafka topics
            ProducerRecord<String, String> flinkRecord = new ProducerRecord<>("flink_topic", movieMessage);
            ProducerRecord<String, String> sparkRecord = new ProducerRecord<>("spark_topic", tripMessage);

            try {
                producer.send(flinkRecord);  // Sending movie data to Flink topic
                producer.send(sparkRecord);  // Sending trip data to Spark topic
                System.out.println("Sent Movie Data: " + movieMessage);  // Optional logging
                System.out.println("Sent Trip Data: " + tripMessage);    // Optional logging

                // Sleep to simulate real-time message generation
                Thread.sleep(10);  // Sleep 10ms between message sends (adjust as needed)
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        producer.close();  // Close the Kafka producer
    }
}
