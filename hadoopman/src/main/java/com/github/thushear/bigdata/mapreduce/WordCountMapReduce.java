package com.github.thushear.bigdata.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by kongming on 2016/10/26.
 */
public class WordCountMapReduce extends Configured implements Tool{


  // Map class

  /**
   * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
   */
  public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text mapperOutKey = new Text();

    private final IntWritable mapperOutValue = new IntWritable(1);

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      super.cleanup(context);
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

      String lineValue = value.toString();

      StringTokenizer stringTokenizer = new StringTokenizer(lineValue);
      while (stringTokenizer.hasMoreTokens()) {
        String token = stringTokenizer.nextToken();
        mapperOutKey.set(token);
        context.write(mapperOutKey, mapperOutValue);
      }
    }
  }


  // reduce class

  /**
   * Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
   */
  public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable outputValue = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      outputValue.set(sum);
      context.write(key, outputValue);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      super.cleanup(context);
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
    }
  }


  // driver component job


  public int run(String[] args) throws Exception {
    // get configuration
    Configuration configuration = new Configuration();

    // create job
    Job job = Job.getInstance(configuration, this.getClass().getSimpleName());

    // run jar
    job.setJarByClass(this.getClass());

    // set job
    // input --> map --> reduce --> output

    // input
    Path inPath = new Path(args[0]);
    FileInputFormat.addInputPath(job, inPath);

    // map
    job.setMapperClass(WordCountMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    //shuffle  start
    // partitioner
//    job.setPartitionerClass(cls);

    //sort
//    job.setSortComparatorClass(cls);
    // combiner
//    job.setCombinerClass(cls);
    // group
//    job.setGroupingComparatorClass(cls);

    //shuffle  end

    // reduce
    job.setReducerClass(WordCountReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // set reducer task num
//    job.setNumReduceTasks();

    // output
    Path outPath = new Path(args[1]);
    FileOutputFormat.setOutputPath(job, outPath);

    // submit job
    boolean result = job.waitForCompletion(true);
    return result ? 0 : 1;

  }


  public static void main(String[] args) throws Exception {
//    int status = new WordCountMapReduce().run(args);
    Configuration configuration = new Configuration();
    int status = ToolRunner.run(configuration,new WordCountMapReduce(),args);
    System.exit(status);
  }


}
