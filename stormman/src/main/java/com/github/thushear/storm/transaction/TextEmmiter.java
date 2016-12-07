package com.github.thushear.storm.transaction;

import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.transactional.ITransactionalSpout;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.tuple.Values;

import java.math.BigInteger;
import java.util.Map;

/**
 * Created by kongming on 2016/12/7.
 */
public class TextEmmiter implements ITransactionalSpout.Emitter<TextMeta> {

  Map<Long,String> _logDB ;

  public TextEmmiter( Map<Long,String> logDB) {
    this._logDB = logDB;
  }

  @Override
  public void emitBatch(TransactionAttempt tx, TextMeta coordinatorMeta, BatchOutputCollector collector) {
     int start = coordinatorMeta.getStart();
     int step = coordinatorMeta.getStep();

    for (long i = start; i < start + step; i++) {
      if (start > _logDB.size())
        return;
      collector.emit(new Values(tx,_logDB.get(i)));
    }
  }

  @Override
  public void cleanupBefore(BigInteger txid) {

  }

  @Override
  public void close() {

  }
}
