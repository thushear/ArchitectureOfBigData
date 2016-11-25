package com.github.thushear.storm.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by kongming on 2016/11/25.
 */
public class WordCountTopology {


  public static class SplitBolt extends BaseBasicBolt{



    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
      String sentence = tuple.getString(0);
      if (StringUtils.isNotBlank(sentence)){
         String[] words = StringUtils.split(sentence," ");

        for (String word : words) {
          System.err.println(Thread.currentThread().getName() + " word = " + word);
          basicOutputCollector.emit(new Values(word));
        }
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
      outputFieldsDeclarer.declare(new Fields("word"));
    }
  }


  public static class WordCountBolt extends BaseBasicBolt{

    Map<String,Integer> counts = new HashMap<String, Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
      String word = tuple.getString(0);
      Integer count = counts.get(word);
      if (count == null)
         count = 0;
      count ++;
      counts.put(word,count);
      System.err.println(Thread.currentThread().getName() + " : word = " + word + " count =" + count);
      basicOutputCollector.emit(new Values(word,count));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word","count"));
    }
  }


  public static class SumBolt extends BaseBasicBolt{

    Map<String,Integer> map = new HashMap<>();
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

      map.put(tuple.getString(0),tuple.getInteger(1));
      int totalValue = 0;
      int keyValue = 0;
      for (Map.Entry<String, Integer> entry : map.entrySet()) {
        System.err.println("key = " + entry.getKey() + "  value = " + entry.getValue());
         totalValue += entry.getValue();
         keyValue ++;
      }
      System.err.println("totalValue=" + totalValue + "  keyValue = " + keyValue);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
  }

  public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("spout",new RandomSentenceSpout());
    builder.setBolt("split",new SplitBolt(),1).shuffleGrouping("spout");
    builder.setBolt("wordcount",new WordCountBolt(),2).fieldsGrouping("split",new Fields("word"));
    builder.setBolt("sumbolt",new SumBolt(),1).shuffleGrouping("wordcount" );
    Config config = new Config();
    config.setDebug(true);
    if (args != null && args.length > 0){
      config.setNumWorkers(3);
      StormSubmitter.submitTopology(args[0],config,builder.createTopology());
    }else {
      config.setMaxTaskParallelism(3);
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count",config,builder.createTopology());
      Utils.sleep(10000);
      cluster.shutdown();
    }

  }





}
