package com.github.ipergenitsa.apache.storm.wordcount;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SplitBolt extends BaseRichBolt {
    private OutputCollector collector;

    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String sentence = input.getStringByField("sentence");
        for (String word : sentence.split(" ")) {
            collector.emit(new Values(word.trim()));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}