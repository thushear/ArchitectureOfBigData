package com.github.thushear.storm.transaction;

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
    System.err.println("txid = " + txid + " " + textMeta);
    return textMeta;
  }

  @Override
  public boolean isReady() {
//    Utils.sleep(2000);
    return true;
  }

  @Override
  public void close() {

  }
}
