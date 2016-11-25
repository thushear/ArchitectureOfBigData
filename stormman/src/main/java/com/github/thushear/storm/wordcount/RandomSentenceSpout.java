package com.github.thushear.storm.wordcount;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * Created by kongming on 2016/11/25.
 */
public class RandomSentenceSpout extends BaseRichSpout {

  SpoutOutputCollector _collector;
  Random _rand;

  @Override
  public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
    _collector = spoutOutputCollector;
    _rand = new Random();
  }

  @Override
  public void nextTuple() {
     String[] words = new String[]{"a b c d","e f g a","b c d e f"};
//     String emitString = words[_rand.nextInt(words.length )];
    for (String sentence : words) {
      System.err.println(Thread.currentThread().getName() + " sentence:" + sentence);
      _collector.emit(new Values(sentence));
    }
    Utils.sleep(10 * 1000);

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("sentence"));
  }
}
