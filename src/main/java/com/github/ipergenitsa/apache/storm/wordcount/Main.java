package com.github.ipergenitsa.apache.storm.wordcount;

import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.LocalCluster;

public class Main {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        SentenceGeneratorSpout sentenceGenerator = new SentenceGeneratorSpout();
        SplitBolt splitter = new SplitBolt();
        CounterBolt counter = new CounterBolt();
        LogWriterBolt logWriter = new LogWriterBolt();

        builder.setSpout("sentenceGenerator", sentenceGenerator);
        builder.setBolt("splitter", splitter).shuffleGrouping("sentenceGenerator");
        builder.setBolt("counter", counter).shuffleGrouping("splitter");
        builder.setBolt("logWriter", logWriter, 2).globalGrouping("counter");
        
        Config config = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("mainTopology", config, builder.createTopology());
        TimeUnit.SECONDS.wait(10);
        cluster.killTopology("mainTopology");
        cluster.shutdown();
    }
}