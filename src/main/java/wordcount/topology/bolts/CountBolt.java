package wordcount.topology.bolts;

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

    @Override
    public void prepare(Map topoConf, TopologyContext context, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.counts = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getStringByField("word");

        int currentCount = counts.getOrDefault(word, 0) + 1;
        counts.put(word, currentCount);

        System.out.println("Result -> Word: [" + word + "] | Count: " + currentCount);

        outputCollector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {}
}
