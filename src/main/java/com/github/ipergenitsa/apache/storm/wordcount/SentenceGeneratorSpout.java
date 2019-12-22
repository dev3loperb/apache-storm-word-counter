package com.github.ipergenitsa.apache.storm.wordcount;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class SentenceGeneratorSpout extends BaseRichSpout {
    private final static String[] sentences = {
        "I want to break free",
        "The show must go on",
        "I want it all",
        "Who wants to live forever",
        "We will rock you"
    };
    
    private SpoutOutputCollector outputCollector;

    public void open(Map<String, Object> config, TopologyContext context, SpoutOutputCollector collector) {
        outputCollector = collector;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

    public void nextTuple() {
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        String randomSentence = sentences[ThreadLocalRandom.current().nextInt(sentences.length)];
        outputCollector.emit(new Values(randomSentence));
    }    
}