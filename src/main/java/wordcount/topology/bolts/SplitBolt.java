package wordcount.topology.bolts;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class SplitBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private transient ObjectMapper objectMapper;

    @Override
    public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void execute(Tuple input) {
        String json = input.getStringByField("value");

        try {
            JsonNode root = objectMapper.readTree(json);
            String paragraph = root.get("text").asText();

            String[] words = paragraph.toLowerCase().split("\\W+");

            for(String word : words) {
                if (!word.isEmpty()) {
                    Values values = new Values(word);
                    outputCollector.emit(input, values);
                }
            }
            System.out.println("----------------------------------------------------------");
            outputCollector.ack(input);
        } catch (Exception e) {
            System.err.println("Failed to parse JSON: " + json);
            outputCollector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
