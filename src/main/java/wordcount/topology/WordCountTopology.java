package wordcount.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import wordcount.topology.bolts.CountBolt;
import wordcount.topology.bolts.SplitBolt;
import wordcount.util.ConfigLoader;
import wordcount.util.KafkaConfigFactory;

public class WordCountTopology {
    public static void main(String[] args) throws Exception {
        // 1. Load configuration
        ConfigLoader loader = new ConfigLoader();

        // 2. Create Kafka Spout configuration
        TopologyBuilder builder = new TopologyBuilder();

        // 3. Create Kafka Spout configuration using the factory
        KafkaSpoutConfig<String, String> spoutConfig = KafkaConfigFactory.getKafkaSpoutConfig(
            loader.getProperty("kafka.bootstrap.servers"),
            loader.getProperty("kafka.topic.input"),
            loader.getProperty("kafka.group.id")
        );

        // 4. Register the Spout (Parallelism = 1)
        builder.setSpout("kafka-spout", new KafkaSpout<>(spoutConfig), 1);

        // 5. Register the Split Bolt (Parallelism from config)
        builder.setBolt("split-bolt", new SplitBolt(), loader.getIntProperty("topology.parallelism.splitter"))
                .shuffleGrouping("kafka-spout");

        // 6. Register the Count Bolt (Parallelism from config) with fields grouping on "word"
        builder.setBolt("count-bolt", new CountBolt(), loader.getIntProperty("topology.parallelism.counter"))
                .fieldsGrouping("split-bolt", new Fields("word"));

        // 7. General Topo Config
        Config config = new Config();
        // These keys are the internal "backdoor" to disable the database that's crashing
        config.put("storm.metricstore.class", "org.apache.storm.metricstore.NoOpMetricStore");
        config.put("storm.metricstore.rocksdb.path", "/tmp/storm_rocksdb");
        // This line specifically tells Nimbus NOT to start the database
        config.put("storm.metricstore.rocksdb.create_if_missing", "false");

        // IMPORTANT: LocalCluster sometimes needs this on Mac to avoid port conflicts
        config.put(Config.TOPOLOGY_DEBUG, false);

        // 8. Run
        LocalCluster cluster = new LocalCluster();
        try {
            System.out.println("### [STORM] Submitting Topology locally... ###");
            cluster.submitTopology("word-count-pipeline", config, builder.createTopology());

            // Run for 10 minutes
            Thread.sleep(600000);

            System.out.println("### [STORM] Shutting down... ###");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            cluster.shutdown(); // Manually shut down
        }
    }
}
