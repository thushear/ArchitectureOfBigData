package com.github.thushear.storm.transaction;

import java.io.Serializable;

/**
 * Created by kongming on 2016/12/7.
 */
public class TextMeta implements Serializable{


  private int start;

  private int step;

  public TextMeta(int start, int step) {
    this.start = start;
    this.step = step;
  }

  public int getStart() {
    return start;
  }

  public void setStart(int start) {
    this.start = start;
  }

  public int getStep() {
    return step;
  }

  public void setStep(int step) {
    this.step = step;
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("TextMeta{");
    sb.append("start=").append(start);
    sb.append(", step=").append(step);
    sb.append('}');
    return sb.toString();
  }
}
