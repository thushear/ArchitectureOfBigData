package com.github.thushear.storm.transaction;

import com.github.thushear.storm.utils.LogFormatter;
import org.apache.storm.transactional.ITransactionalSpout;
import org.apache.storm.utils.Utils;

import java.math.BigInteger;

/**
 * Created by kongming on 2016/12/7.
 */
public class TextCoordinator implements ITransactionalSpout.Coordinator<TextMeta> {

  @Override
  public TextMeta initializeTransaction(BigInteger txid, TextMeta prevMetadata) {
    int start = 0;
    if (prevMetadata == null){
       start = 0;
    }else {
      start = prevMetadata.getStart() + prevMetadata.getStep();
    }
    if (start > 100){
      Utils.sleep(10000);
    }

    TextMeta textMeta = new TextMeta(start,10);
//    System.err.println( "ThreadId "  "txid = " + txid + " " + textMeta);
    LogFormatter.trace(" Class %s Method %s  ThreadId %s ThreadName %s txId %s textMeta %s \n", this.getClass().getName(), "initializeTransaction", Thread.currentThread().getId(), Thread.currentThread().getName(), txid, textMeta);
    return textMeta;
  }

  @Override
  public boolean isReady() {
//    Utils.sleep(2000);
    LogFormatter.trace(" Class %s Method %s  ThreadId %s ThreadName %s \n",this.getClass().getName(),"isReady", Thread.currentThread().getId(),Thread.currentThread().getName());
    return true;
  }

  @Override
  public void close() {

  }
}
