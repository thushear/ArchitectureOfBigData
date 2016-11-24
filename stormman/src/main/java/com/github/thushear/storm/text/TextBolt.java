package com.github.thushear.storm.text;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by kongming on 2016/11/24.
 */
public class TextBolt implements IRichBolt {

  int num = 0;

  OutputCollector outputCollector;

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.outputCollector = outputCollector;
  }

  @Override
  public void execute(Tuple tuple) {
    try {
      String tupleValue = tuple.getStringByField("log");
      if (tupleValue != null){
        System.out.println("-------tupleValue =" + tupleValue);
        num ++;
      }
      outputCollector.ack(tuple);
    } catch (Exception e) {
      outputCollector.fail(tuple);
      e.printStackTrace();
    }
  }

  @Override
  public void cleanup() {

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("result"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
