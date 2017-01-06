package com.github.thushear.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by kongming on 2017/1/2.
  */
object SimpleStreamingApp {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("streaming")
    //if spark shell
    //val ssc = new StreamingContext(sc,Seconds(10))
    val ssc = new StreamingContext(conf,Seconds(10))

    val lines = ssc.textFileStream("hdfs://hadoop-master:9000/spark/streaming")
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map((_,1)).reduceByKey(_ + _)
    wordCounts.saveAsTextFiles("hdfs://hadoop-master:9000/spark/output")
    ssc.start()
    ssc.awaitTermination()

  }


}
