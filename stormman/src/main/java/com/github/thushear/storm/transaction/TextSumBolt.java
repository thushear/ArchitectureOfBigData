package com.github.thushear.storm.transaction;

import com.github.thushear.storm.utils.LogFormatter;
import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseTransactionalBolt;
import org.apache.storm.transactional.ICommitter;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.tuple.Tuple;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by kongming on 2016/12/7.
 */
public class TextSumBolt extends BaseTransactionalBolt implements ICommitter{

  int sum = 0;

  int totalSum;

  BigInteger _txid;

  private static Map<String,DBEntry> sumDB = new HashMap<>();

  @Override
  public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, TransactionAttempt id) {
     this._txid = id.getTransactionId();
    LogFormatter.trace(" Class %s Method %s  ThreadId %s ThreadName %s  TransactionAttempt %s \n ",this.getClass().getName(),"prepare", Thread.currentThread().getId(),Thread.currentThread().getName(),id);
  }

  @Override
  public void execute(Tuple tuple) {
    TransactionAttempt   transactionAttempt = (TransactionAttempt) tuple.getValue(0);

    int count = tuple.getInteger(1);
    sum += count;
    LogFormatter.trace(" Class %s Method %s  ThreadId %s ThreadName %s  TransactionAttempt %s  sum %s \n",this.getClass().getName(),"execute", Thread.currentThread().getId(),Thread.currentThread().getName(),transactionAttempt,sum);
  }

  @Override
  public void finishBatch() {
     DBEntry dbEntry = sumDB.get("sum");
    if (dbEntry == null || !dbEntry.getTxId().equals(_txid) ){
      if (dbEntry == null){
        dbEntry = new DBEntry();
        dbEntry.setTxId(_txid);
        dbEntry.setSum(sum);
        sumDB.put("sum",dbEntry);
      }else {

        dbEntry.setSum(dbEntry.getSum() + sum);
        dbEntry.setTxId(_txid);
        sumDB.put("sum",dbEntry);
      }
    }
    LogFormatter.trace(" Class %s Method %s  ThreadId %s ThreadName %s  dbEntry %s \n  ",this.getClass().getName(),"finishBatch", Thread.currentThread().getId(),Thread.currentThread().getName(),dbEntry);

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }


  public static class DBEntry{

    private BigInteger txId;

    private Integer sum;

    public Integer getSum() {
      return sum;
    }

    public void setSum(Integer sum) {
      this.sum = sum;
    }

    public BigInteger getTxId() {
      return txId;
    }

    public void setTxId(BigInteger txId) {
      this.txId = txId;
    }

    @Override
    public String toString() {
      final StringBuffer sb = new StringBuffer("DBEntry{");
      sb.append("sum=").append(sum);
      sb.append(", txId=").append(txId);
      sb.append('}');
      return sb.toString();
    }
  }
}
