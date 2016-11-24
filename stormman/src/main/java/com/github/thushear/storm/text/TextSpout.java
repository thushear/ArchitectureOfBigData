package com.github.thushear.storm.text;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

/**
 * Created by kongming on 2016/11/24.
 */
public class TextSpout implements IRichSpout {

  BufferedReader bufferedReader;

  String line;

  SpoutOutputCollector spoutOutputCollector;

  @Override
  public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
    try {
      this.spoutOutputCollector = spoutOutputCollector;
      this.bufferedReader = new BufferedReader(new FileReader(TextSpout.class.getResource("/").getPath() + "web.log"));

    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void close() {

  }

  @Override
  public void activate() {

  }

  @Override
  public void deactivate() {

  }

  @Override
  public void nextTuple() {
    try {
      while ((line = bufferedReader.readLine()) != null) {
        System.err.println("line = " + line);
        spoutOutputCollector.emit(new Values(line));
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void ack(Object o) {

  }

  @Override
  public void fail(Object o) {

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("log"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }


  public static void main(String[] args) throws IOException {

    BufferedReader bufferedReader = new BufferedReader(new FileReader(TextSpout.class.getResource("/").getPath() + "web.log"));

    System.out.println(bufferedReader.readLine());
  }
}
