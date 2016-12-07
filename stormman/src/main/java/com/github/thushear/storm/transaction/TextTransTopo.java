package com.github.thushear.storm.transaction;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.transactional.TransactionalTopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * Created by kongming on 2016/12/7.
 */
public class TextTransTopo {

  public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
    TransactionalTopologyBuilder transactionalTopologyBuilder = new TransactionalTopologyBuilder("ttb", "textspout", new TextTransactionalSpout(), 1);
    transactionalTopologyBuilder.setBolt("textcountbolt", new TextCountgBolt(), 1).shuffleGrouping("textspout");
//    transactionalTopologyBuilder.setCommitterBolt("textsumbolt", new TextSumBolt(), 1).shuffleGrouping("textcountbolt");
    transactionalTopologyBuilder.setBolt("textsumbolt", new TextSumBolt(), 1).shuffleGrouping("textcountbolt");
    Config config = new Config();
    config.setDebug(true);
    if (args != null && args.length > 0){
      config.setNumWorkers(3);
      StormSubmitter.submitTopology(args[0],config,transactionalTopologyBuilder.buildTopology());
    }else {
      config.setMaxTaskParallelism(3);
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("texttransaction",config,transactionalTopologyBuilder.buildTopology());
      Utils.sleep(10000);
      cluster.shutdown();
    }

  }
}
