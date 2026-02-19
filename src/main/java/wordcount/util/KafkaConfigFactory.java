package wordcount.util;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class KafkaConfigFactory {

    public static KafkaSpoutConfig<String, String> getKafkaSpoutConfig(String bootstrapServers, String topic, String groupId) {
        KafkaSpoutConfig.Builder<String, String> builder = KafkaSpoutConfig.builder(bootstrapServers, topic);

        // Set the consumer group ID - REQUIRED for Kafka consumer
        builder.setProp(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // Manual property setting to avoid the FirstPollOffsetStrategy import error
        // "earliest" is the string equivalent of FirstPollOffsetStrategy.EARLIEST
        builder.setProp(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        builder.setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        builder.setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return builder.build();
    }
}