package com.github.thushear.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by kongming on 2016/12/28.
  */
object SimpleApp {


  def main(args: Array[String]) {

    println("hello world!")

//    val conf = new SparkConf().setAppName("simple")
//    val spark = new SparkContext(conf)
//    val rdd = spark.textFile("hdfs://hadoop-master:9000/user/root/score.input")
//    val groupTopKeyRdd = rdd.map(_.split(" ")).map(x => (x(0),x(1))).groupByKey.map(
//      x => {
//        val xx = x._1
//        val yy = x._2
//        (xx,yy.toList.sorted.reverse.take(3))
//      }
//    )
//
//    groupTopKeyRdd.saveAsTextFile("hdfs://hadoop-master:9000/user/root/spark/output")
//    spark.stop()







  }



}
