package com.github.ipergenitsa.apache.storm.wordcount;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CounterBolt extends BaseRichBolt {
    private final Map<String, Integer> counts = new HashMap<>();
    private OutputCollector outputCollector;

    public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
    }

    public void execute(Tuple input) {
        String word = input.getStringByField("word");
        counts.merge(word, 1, (a, b) -> a + b);
        outputCollector.emit(new Values(word, counts.get(word)));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}