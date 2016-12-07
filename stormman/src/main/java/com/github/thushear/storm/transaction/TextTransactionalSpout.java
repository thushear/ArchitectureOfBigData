package com.github.thushear.storm.transaction;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.transactional.ITransactionalSpout;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by kongming on 2016/12/7.
 */
public class TextTransactionalSpout implements ITransactionalSpout<TextMeta> {

  private Map<Long,String> logDB;

  String[] words = new String[]{"a b c d","e f g a","b c d e f"};

  public TextTransactionalSpout() {
    logDB = new HashMap<>();
    Random rand = new Random();
    for (long i = 0; i < 100; i++) {
      logDB.put(i,words[rand.nextInt(2)]);
    }

  }

  @Override
  public Coordinator<TextMeta> getCoordinator(Map conf, TopologyContext context) {
    return new TextCoordinator();
  }

  @Override
  public Emitter<TextMeta> getEmitter(Map conf, TopologyContext context) {
    return new TextEmmiter(logDB);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("tx","log"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
