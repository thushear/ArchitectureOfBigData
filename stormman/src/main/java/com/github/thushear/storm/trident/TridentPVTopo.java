package com.github.thushear.storm.trident;

import com.github.thushear.storm.utils.LogFormatter;
import com.github.thushear.storm.utils.MockData;
import com.google.common.base.Function;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.shade.com.google.common.collect.Lists;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Arrays;
import java.util.Random;

import static com.github.thushear.storm.utils.MockData.mockLineList;

/**
 * Created by kongming on 2016/12/11.
 */
public class TridentPVTopo {


  public static class Split extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
      String sentence = tuple.getString(0);
      for (String word : sentence.split(" ")) {
        collector.emit(new Values(word));
      }
    }
  }

  public static StormTopology buildTopology(LocalDRPC drpc) {
//    FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3, new Values("the cow jumped over the moon"),
//      new Values("the man went to the store and bought some candy"), new Values("four score and seven years ago"),
//      new Values("how many apples can you eat"), new Values("to be or not to be the person"));
//    spout.setCycle(true);

    // Log Spout
//    FixedBatchSpout logSpout = new FixedBatchSpout(new Fields("log"),3, com.google.common.collect.Lists.transform(mockLineList, new Function<String, Object>() {
//      @Override
//      public Object apply(String s) {
//        return new Values(s);
//      }
//    }));
    Random random = new Random();
    FixedBatchSpout logSpout = new FixedBatchSpout(new Fields("log"),3
      ,new Values(mockLineList.get(random.nextInt(mockLineList.size() - 1)))
      ,new Values(mockLineList.get(random.nextInt(mockLineList.size() - 1)))
      ,new Values(mockLineList.get(random.nextInt(mockLineList.size() - 1))));



    logSpout.setCycle(true);

    TridentTopology topology = new TridentTopology();
    TridentState logState = topology.newStream("logSpout", logSpout)
//      .parallelismHint(16)
      .each(new Fields("log"),new LogSplit(), new Fields("ip","date"))
      .groupBy(new Fields("date"))
      .persistentAggregate(new MemoryMapState.Factory(), new Fields("ip"), new Count(), new Fields("count"));
//      .parallelismHint(16);

    topology.newDRPCStream("getPV", drpc)
      .each(new Fields("args"), new Split(), new Fields("date"))
      .groupBy(new Fields("date"))
      .stateQuery(logState, new Fields("date"), new MapGet(), new Fields("count"))
      .each(new Fields("count"), new FilterNull())
      .aggregate(new Fields("count"), new Sum(), new Fields("sum"));
    return topology.build();
  }


  public static void main(String[] args) throws Exception {
    Config conf = new Config();
    conf.setMaxSpoutPending(20);
    if (args.length == 0) {
      LocalDRPC drpc = new LocalDRPC();
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("logTopo", conf, buildTopology(drpc));
      for (int i = 0; i < 100; i++) {
        LogFormatter.trace("DRPC RESULT: %s \n",drpc.execute("getPV", "2014-05-04"));
        //System.out.println("DRPC RESULT: " + drpc.execute("words", "cat the dog jumped"));
        Thread.sleep(1000);
      }
    } else {
      conf.setNumWorkers(3);
      StormSubmitter.submitTopology(args[0], conf, buildTopology(null));
    }
  }


}
