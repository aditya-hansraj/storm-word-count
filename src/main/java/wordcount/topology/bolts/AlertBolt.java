package wordcount.topology.bolts;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class AlertBolt extends BaseBasicBolt {
    private static final Set<String> DANGER_WORDS = new HashSet<>(Arrays.asList("critical", "error", "failure", "crash", "danger"));

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String word = input.getStringByField("word");

        if (DANGER_WORDS.contains(word.toLowerCase())) {
            System.out.println("\n###############################################");
            System.out.println("!!! ALERT BOLT DETECTED CRITICAL WORD: " + word.toUpperCase() + " !!!");
            System.out.println("###############################################\n");
        }
    }

    @Override
    public void declareOutputFields(org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer) {}

}
