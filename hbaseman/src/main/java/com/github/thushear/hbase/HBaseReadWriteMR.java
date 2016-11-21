package com.github.thushear.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * http://hbase.apache.org/book.html#mapreduce
 * HBase 从一张表读取数据写入到另一张表MR实例
 *  export HBASE_HOME=/usr/local/hbase-0.98.6-hadoop2
    export HADOOP_HOME=/usr/local/hadoop
    HADOOP_CLASSPATH=`${HBASE_HOME}/bin/hbase mapredcp` $HADOOP_HOME/bin/yarn jar /usr/local/hadoop/examples/hbaseman-0.0.1-SNAPSHOT.jar
 *  Created by kongming on 2016/11/20.
 */
public class HBaseReadWriteMR extends Configured implements Tool {

  /**
   * Read Mapper
   */
  public static class ReadTableMapper extends TableMapper<Text, Put> {

    private Text text = new Text();

    @Override
    public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

      text.set(Bytes.toString(key.get()));
      Put put = new Put(key.get());
      for (Cell cell : value.rawCells()) {
         if ("info".equals(Bytes.toString(CellUtil.cloneFamily(cell))) && "name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
           put.add(cell);
        }
      }
      context.write(text,put);
    }
  }

  /**
   *   Write Reducer
   */
  public static class WriteTableReducer extends TableReducer<Text,Put,ImmutableBytesWritable>{

    @Override
    public void reduce(Text key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
      for (Put put : values) {
         context.write(null,put);
      }
    }
  }




  public int run(String[] args) throws Exception {
    Configuration configuration = this.getConf();
    Job job = new Job(configuration,"readWriteDataExample");
    job.setJarByClass(HBaseReadWriteMR.class);

    Scan scan = new Scan();
    scan.setCaching(500);
    scan.setCacheBlocks(false);

    TableMapReduceUtil.initTableMapperJob(
      "user",
       scan,
      ReadTableMapper.class,
      Text.class,
      Put.class,
      job
    );

    TableMapReduceUtil.initTableReducerJob(
      "simple_user",
      WriteTableReducer.class,
      job
    );

    job.setNumReduceTasks(1);
    boolean isSuccess = job.waitForCompletion(true);
    return isSuccess ? 1:0;
  }


  public static void main(String[] args) throws Exception {
    Configuration configuration = HBaseConfiguration.create();
    int status = ToolRunner.run(configuration,new HBaseReadWriteMR(),args);
    System.exit(status);
  }


}
