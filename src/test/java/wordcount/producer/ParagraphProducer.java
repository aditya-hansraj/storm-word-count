package wordcount.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;

public class ParagraphProducer {
    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String topic = "word-input";
        // We use the filename only, because ClassLoader finds it in the resources folder
        String fileName = "data.jsonl";

        // 1. Kafka Producer Properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2. Load paragraphs using ClassLoader (Works in IDE and during tests)
        List<String> paragraphs;
        try (InputStream is = ParagraphProducer.class.getClassLoader().getResourceAsStream(fileName)) {
            if (is == null) {
                System.err.println("Error: Could not find " + fileName + " in resources!");
                return;
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
                paragraphs = reader.lines().collect(Collectors.toList());
            }
        } catch (Exception e) {
            System.err.println("Error reading the data file: " + e.getMessage());
            return;
        }

        // 3. Start Kafka Producer
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("### Starting Producer from " + fileName + ". Press Ctrl+C to stop. ###");
            Random random = new Random();

            while (true) {
                // Pick random line from the loaded list
                String message = paragraphs.get(random.nextInt(paragraphs.size()));

                // Send to Kafka
                producer.send(new ProducerRecord<>(topic, message));

                System.out.println("Produced from file: " + message);

                // Delay to simulate streaming (currently set to 10 seconds)
                Thread.sleep(1);
            }
        } catch (InterruptedException e) {
            System.out.println("Producer interrupted. Shutting down...");
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            System.err.println("Kafka Producer Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}