package com.github.thushear.hive.udf;

/**
 * Created by kongming on 2016/11/3.
 */

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * First, you need to create a new class that extends UDF, with one or more methods named evaluate.
 */
public class MyLowerUDF extends UDF {

  public Text evaluate(Text s){
    if (s == null) return null;
    return new Text(s.toString().toLowerCase());
  }


}
