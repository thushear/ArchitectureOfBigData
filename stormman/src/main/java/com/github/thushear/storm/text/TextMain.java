package com.github.thushear.storm.text;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by kongming on 2016/11/24.
 */
public class TextMain {


  public static void main(String[] args) {

    TopologyBuilder topologyBuilder = new TopologyBuilder();
    topologyBuilder.setSpout("textSpout",new TextSpout(),1);
    topologyBuilder.setBolt("textBolt",new TextBolt(),1).shuffleGrouping("textSpout");
    Map conf = new HashMap<>();
    conf.put(Config.TOPOLOGY_WORKERS,4);

    if (args.length>0){

      try {
        StormSubmitter.submitTopology(args[0],conf,topologyBuilder.createTopology());
      } catch (AlreadyAliveException e) {
        e.printStackTrace();
      } catch (InvalidTopologyException e) {
        e.printStackTrace();
      } catch (AuthorizationException e) {
        e.printStackTrace();
      }
    }else {

      LocalCluster localCluster = new LocalCluster();
      localCluster.submitTopology("local",conf,topologyBuilder.createTopology());

    }


  }



}
