package com.github.thushear.spark.product;

import org.apache.spark.sql.api.java.UDF3;

/**将两个字段拼接起来（使用指定的分隔符）
 * Created by kongming on 2017/2/9.
 */
public class ConcatLongStringUDF  implements UDF3<Long,String,String,String> {
  @Override
  public String call(Long aLong, String s, String split) throws Exception {
    return String.valueOf(aLong) + split + s;
  }
}
