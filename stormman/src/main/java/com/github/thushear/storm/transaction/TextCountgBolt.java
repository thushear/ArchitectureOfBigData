package com.github.thushear.storm.transaction;

import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseTransactionalBolt;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by kongming on 2016/12/7.
 */
public class TextCountgBolt extends BaseTransactionalBolt  {

  int count;

  BatchOutputCollector _collector;

  TransactionAttempt _id;

  @Override
  public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, TransactionAttempt id) {
    System.err.println("TextCountgBolt prepare id " + id);
    this._collector = collector;
    this._id = id;
  }

  @Override
  public void execute(Tuple tuple) {
    TransactionAttempt transactionAttempt = (TransactionAttempt) tuple.getValue(0);
    System.err.println("transactionAttempt " + transactionAttempt.getAttemptId() + "  " + transactionAttempt.getTransactionId());
    String log = tuple.getString(1);
    if (StringUtils.isNotBlank(log)){
      count ++;
    }
  }

  @Override
  public void finishBatch() {
    _collector.emit(new Values(_id,count));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("txid","count"));
  }
}
