package com.github.ipergenitsa.apache.storm.wordcount;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogWriterBolt extends BaseRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(LogWriterBolt.class);

    public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
    }

    public void execute(Tuple input) {
        String word = input.getStringByField("word");
        Integer count = input.getIntegerByField("count");
        logger.info("count of word: '{}' is = {}", word, count);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}