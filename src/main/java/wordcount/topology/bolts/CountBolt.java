package wordcount.topology.bolts;

import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class CountBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private Map<String, Integer> counts;
    private transient MultiCountMetric wordProcessingMetric;

    @Override
    public void prepare(Map topoConf, TopologyContext context, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.counts = new HashMap<>();
        this.wordProcessingMetric = new MultiCountMetric();

        context.registerMetric("word-counts", wordProcessingMetric, 60);
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getStringByField("word");
        simulateBottleneck();
        int currentCount = counts.getOrDefault(word, 0) + 1;
        counts.put(word, currentCount);

        wordProcessingMetric.scope(word).incr();

        System.out.println("Result -> Word: [" + word + "] | Count: " + currentCount);

        outputCollector.ack(input);
    }

    void simulateBottleneck() {
        try {
            Thread.sleep(200); // Simulate processing delay
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {}
}
