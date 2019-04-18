package com.bde.twitter_storm;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import twitter4j.Status;

public class TweetStripBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

    }

    @Override
    public void execute(Tuple input) {
        Status tweet = (Status) input.getValueByField("tweet");
        boolean isEn = tweet.getLang().equals("en") ? true : false;
        boolean isRT = tweet.isRetweet();
        //filter out image only tweets?
        if(isEn && !isRT) {
            collector.emit(new Values(tweet.getCreatedAt(), String.valueOf(tweet.getId()), tweet.getText()));
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("created_at", "id", "text"));
    }
    
}