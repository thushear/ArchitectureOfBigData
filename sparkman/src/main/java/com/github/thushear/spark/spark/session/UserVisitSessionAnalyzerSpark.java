package com.github.thushear.spark.spark.session;

/**
 * Created by kongming on 2017/1/8.
 */

import com.alibaba.fastjson.JSONObject;
import com.github.thushear.spark.conf.ConfigurationManager;
import com.github.thushear.spark.constant.Constants;
import com.github.thushear.spark.dao.ISessionAggrStatDAO;
import com.github.thushear.spark.dao.ISessionDetailDAO;
import com.github.thushear.spark.dao.ISessionRandomExtractDAO;
import com.github.thushear.spark.dao.ITaskDAO;
import com.github.thushear.spark.dao.factory.DAOFactory;
import com.github.thushear.spark.domain.SessionAggrStat;
import com.github.thushear.spark.domain.SessionDetail;
import com.github.thushear.spark.domain.SessionRandomExtract;
import com.github.thushear.spark.domain.Task;
import com.github.thushear.spark.test.MockData;
import com.github.thushear.spark.util.*;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.*;

/**
 * 用户访问session分析Spark作业
 *
 * 接收用户创建的分析任务，用户可能指定的条件如下：
 *
 * 1、时间范围：起始日期~结束日期
 * 2、性别：男或女
 * 3、年龄范围
 * 4、职业：多选
 * 5、城市：多选
 * 6、搜索词：多个搜索词，只要某个session中的任何一个action搜索过指定的关键词，那么session就符合条件
 * 7、点击品类：多个品类，只要某个session中的任何一个action点击过某个品类，那么session就符合条件
 *
 * 我们的spark作业如何接受用户创建的任务？
 *
 * J2EE平台在接收用户创建任务的请求之后，会将任务信息插入MySQL的task表中，任务参数以JSON格式封装在task_param
 * 字段中
 *
 * 接着J2EE平台会执行我们的spark-submit shell脚本，并将taskid作为参数传递给spark-submit shell脚本
 * spark-submit shell脚本，在执行时，是可以接收参数的，并且会将接收的参数，传递给Spark作业的main函数
 * 参数就封装在main函数的args数组中
 *
 * 这是spark本身提供的特性
 *
 * @author Administrator
 *
 */

public class UserVisitSessionAnalyzerSpark {


  public static void main(String[] args) {

    SparkConf conf = new SparkConf()
      .setAppName(Constants.SPARK_APP_NAME_SESSION)
      .setMaster("local");

    JavaSparkContext sc = new JavaSparkContext(conf);
    SQLContext sqlContext = getSQLContext(sc.sc());
    // 创建需要使用的DAO组件
    ITaskDAO taskDAO = DAOFactory.getTaskDAO();
    mockData(sc,sqlContext);
// 首先得查询出来指定的任务，并获取任务的查询参数
    long taskId = ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_SESSION);
    Task task = taskDAO.findById(taskId);
    if (task == null) {
      System.out.println(new Date( ) + ":cannot find this task with id [" + taskId + "].");
      return;
    }

    JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
    // 如果要进行session粒度的数据聚合
    // 首先要从user_visit_action表中，查询出来指定日期范围内的行为数据

    JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext,taskParam);
    JavaPairRDD<String,Row> session2ActionRDD = getSessionid2ActionRDD(actionRDD);
    JavaPairRDD<String,String> sessionId2FullAggrInfoRDD = aggregateBySession(actionRDD,sqlContext);
    System.out.println("sessionId2FullAggrInfoRDD=" + sessionId2FullAggrInfoRDD.count());
    List<Tuple2<String,String>>  list = sessionId2FullAggrInfoRDD.take(10);
    for (Tuple2<String, String> tuple2 : list) {
      System.out.println(tuple2._1 + ":" + tuple2._2);
    }

    Accumulator<String> sessionAggrStatAccumulator = sc.accumulator("",new SessionAggrStatAccumulator());
    JavaPairRDD<String,String> filteredSessionId2AggrRDD = filterSession(sessionId2FullAggrInfoRDD,taskParam,sessionAggrStatAccumulator);

    System.out.println("filteredSessionId2AggrRDD= " + filteredSessionId2AggrRDD.count());
    for (Tuple2<String, String> tuple2 : filteredSessionId2AggrRDD.take(10)) {
      System.out.println(tuple2._1 + ":" + tuple2._2);
    }
    /**
     * 对于Accumulator这种分布式累加计算的变量的使用，有一个重要说明
     *
     * 从Accumulator中，获取数据，插入数据库的时候，一定要，一定要，是在有某一个action操作以后
     * 再进行。。。
     *
     * 如果没有action的话，那么整个程序根本不会运行。。。
     *
     * 是不是在calculateAndPersisitAggrStat方法之后，运行一个action操作，比如count、take
     * 不对！！！
     *
     * 必须把能够触发job执行的操作，放在最终写入MySQL方法之前
     *
     * 计算出来的结果，在J2EE中，是怎么显示的，是用两张柱状图显示
     */

    System.out.println(filteredSessionId2AggrRDD.count());


    extractRandomSession(filteredSessionId2AggrRDD,taskId,session2ActionRDD);

    // 计算出各个范围的session占比，并写入MySQL
    calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(),taskId);
    /**
     * session聚合统计（统计出访问时长和访问步长，各个区间的session数量占总session数量的比例）
     *
     * 如果不进行重构，直接来实现，思路：
     * 1、actionRDD，映射成<sessionid,Row>的格式
     * 2、按sessionid聚合，计算出每个session的访问时长和访问步长，生成一个新的RDD
     * 3、遍历新生成的RDD，将每个session的访问时长和访问步长，去更新自定义Accumulator中的对应的值
     * 4、使用自定义Accumulator中的统计值，去计算各个区间的比例
     * 5、将最后计算出来的结果，写入MySQL对应的表中
     *
     * 普通实现思路的问题：
     * 1、为什么还要用actionRDD，去映射？其实我们之前在session聚合的时候，映射已经做过了。多此一举
     * 2、是不是一定要，为了session的聚合这个功能，单独去遍历一遍session？其实没有必要，已经有session数据
     * 		之前过滤session的时候，其实，就相当于，是在遍历session，那么这里就没有必要再过滤一遍了
     *
     * 重构实现思路：
     * 1、不要去生成任何新的RDD（处理上亿的数据）
     * 2、不要去单独遍历一遍session的数据（处理上千万的数据）
     * 3、可以在进行session聚合的时候，就直接计算出来每个session的访问时长和访问步长
     * 4、在进行过滤的时候，本来就要遍历所有的聚合session信息，此时，就可以在某个session通过筛选条件后
     * 		将其访问时长和访问步长，累加到自定义的Accumulator上面去
     * 5、就是两种截然不同的思考方式，和实现方式，在面对上亿，上千万数据的时候，甚至可以节省时间长达
     * 		半个小时，或者数个小时
     *
     * 开发Spark大型复杂项目的一些经验准则：
     * 1、尽量少生成RDD
     * 2、尽量少对RDD进行算子操作，如果有可能，尽量在一个算子里面，实现多个需要做的功能
     * 3、尽量少对RDD进行shuffle算子操作，比如groupByKey、reduceByKey、sortByKey（map、mapToPair）
     * 		shuffle操作，会导致大量的磁盘读写，严重降低性能
     * 		有shuffle的算子，和没有shuffle的算子，甚至性能，会达到几十分钟，甚至数个小时的差别
     * 		有shfufle的算子，很容易导致数据倾斜，一旦数据倾斜，简直就是性能杀手（完整的解决方案）
     * 4、无论做什么功能，性能第一
     * 		在传统的J2EE或者.NET后者PHP，软件/系统/网站开发中，我认为是架构和可维护性，可扩展性的重要
     * 		程度，远远高于了性能，大量的分布式的架构，设计模式，代码的划分，类的划分（高并发网站除外）
     *
     * 		在大数据项目中，比如MapReduce、Hive、Spark、Storm，我认为性能的重要程度，远远大于一些代码
     * 		的规范，和设计模式，代码的划分，类的划分；大数据，大数据，最重要的，就是性能
     * 		主要就是因为大数据以及大数据项目的特点，决定了，大数据的程序和项目的速度，都比较慢
     * 		如果不优先考虑性能的话，会导致一个大数据处理程序运行时间长度数个小时，甚至数十个小时
     * 		此时，对于用户体验，简直就是一场灾难
     *
     * 		所以，推荐大数据项目，在开发和代码的架构中，优先考虑性能；其次考虑功能代码的划分、解耦合
     *
     * 		我们如果采用第一种实现方案，那么其实就是代码划分（解耦合、可维护）优先，设计优先
     * 		如果采用第二种方案，那么其实就是性能优先
     *
     * 		讲了这么多，其实大家不要以为我是在岔开话题，大家不要觉得项目的课程，就是单纯的项目本身以及
     * 		代码coding最重要，其实项目，我觉得，最重要的，除了技术本身和项目经验以外；非常重要的一点，就是
     * 		积累了，处理各种问题的经验
     *
     */



    sc.close();

  }

  /**
   * 获取sessionid2到访问行为数据的映射的RDD
   * @param actionRDD
   * @return
   */
  public static JavaPairRDD<String, Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD) {
    return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {

      private static final long serialVersionUID = 1L;

      @Override
      public Tuple2<String, Row> call(Row row) throws Exception {
        return new Tuple2<String, Row>(row.getString(2), row);
      }

    });
  }


  private static void extractRandomSession(JavaPairRDD<String,String> sessionAggrInfoRDD,long taskid,JavaPairRDD<String, Row> sessionid2actionRDD){
    // 第一步，计算出每天每小时的session数量，获取<yyyy-MM-dd_HH,sessionid>格式的RDD
    JavaPairRDD<String,String> dateHour2AggrRDD = sessionAggrInfoRDD.mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {
      @Override
      public Tuple2<String, String> call(Tuple2<String, String> tuple2) throws Exception {
        String aggrInfo = tuple2._2;
        String startTime = StringUtils.getFieldFromConcatString(aggrInfo,"\\|",Constants.FIELD_START_TIME);
        String dateHour = DateUtils.getDateHour(startTime);

        return new Tuple2<String, String>(dateHour,aggrInfo);
      }
    });

    /**
     * 思考一下：这里我们不要着急写大量的代码，做项目的时候，一定要用脑子多思考
     *
     * 每天每小时的session数量，然后计算出每天每小时的session抽取索引，遍历每天每小时session
     * 首先抽取出的session的聚合数据，写入session_random_extract表
     * 所以第一个RDD的value，应该是session聚合数据
     *
     */
    Map<String,Object> hourCountMap = dateHour2AggrRDD.countByKey();

    // 第二步，使用按时间比例随机抽取算法，计算出每天每小时要抽取session的索引
    // 将<yyyy-MM-dd_HH,count>格式的map，转换成<yyyy-MM-dd,<HH,count>>的格式
    Map<String,Map<String,Long>> dateHourCountMap = new HashMap<>();
    for (Map.Entry<String, Object> entry : hourCountMap.entrySet()) {
      String dateHour = entry.getKey();
      String date = dateHour.split("_")[0];
      String hour = dateHour.split("_")[1];
      long count = Long.valueOf(String.valueOf(entry.getValue()) );
      Map<String,Long> hourMap = dateHourCountMap.get(date);
      if (hourMap == null){
        hourMap = new HashMap<>();
        dateHourCountMap.put(date,hourMap);
      }
      hourMap.put(hour,count);


    }

    // 开始实现我们的按时间比例随机抽取算法

    // 总共要抽取100个session，先按照天数，进行平分
    int extractNumberPerDay = 100 / dateHourCountMap.size();

    Map<String,Map<String,List<Integer>>>  dateHourExtractMap
        = new HashMap<>();

    Random random = new Random();

    for (Map.Entry<String, Map<String, Long>> entry : dateHourCountMap.entrySet()) {
       String date = entry.getKey();
       Map<String,Long> hourMap =   entry.getValue();

      long sessionCount = 0;
      for (Long hourCount : hourMap.values()) {
         sessionCount += hourCount;
      }
      Map<String,List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
      if (hourExtractMap == null){
        hourExtractMap = new HashMap<>();
        dateHourExtractMap.put(date,hourExtractMap);
      }

      for (Map.Entry<String, Long> hourCountEntry : hourMap.entrySet()) {
        String hour = hourCountEntry.getKey();
        long count = hourCountEntry.getValue();
// 计算每个小时的session数量，占据当天总session数量的比例，直接乘以每天要抽取的数量
        // 就可以计算出，当前小时需要抽取的session数量
        int hourExtractNumber = (int)(((double)count / (double)sessionCount)
          * extractNumberPerDay);
        List<Integer> extractIndexList = hourExtractMap.get(hour);
        if (extractIndexList == null) {
          extractIndexList = new ArrayList();
          hourExtractMap.put(hour,extractIndexList);
        }

        for (int i = 0; i < hourExtractNumber; i++) {
          int extractIndex = random.nextInt((int)count);
          while (extractIndexList.contains(extractIndex)){
            extractIndex = random.nextInt((int)count);
          }
          extractIndexList.add(extractIndex);
        }

      }


    }

    /**
     * 第三步：遍历每天每小时的session，然后根据随机索引进行抽取
     */

    // 执行groupByKey算子，得到<dateHour,(session aggrInfo)>
    JavaPairRDD<String,Iterable<String>> time2SessionsRDD = dateHour2AggrRDD.groupByKey();
    // 我们用flatMap算子，遍历所有的<dateHour,(session aggrInfo)>格式的数据
    // 然后呢，会遍历每天每小时的session
    // 如果发现某个session恰巧在我们指定的这天这小时的随机抽取索引上
    // 那么抽取该session，直接写入MySQL的random_extract_session表
    // 将抽取出来的session id返回回来，形成一个新的JavaRDD<String>
    // 然后最后一步，是用抽取出来的sessionid，去join它们的访问行为明细数据，写入session表
    JavaPairRDD<String,String > extractSessionidsRDD = time2SessionsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<String>>, String, String>() {

      @Override
      public Iterable<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> tuple) throws Exception {
        List<Tuple2<String,String>> extractSessionIds = new ArrayList<Tuple2<String, String>>();

        String dateHour = tuple._1;
        String date = dateHour.split("_")[0];
        String hour = dateHour.split("_")[1];
        Iterator<String> iterator = tuple._2.iterator();
        List<Integer> extractIndexList = dateHourExtractMap.get(date).get(hour);
        ISessionRandomExtractDAO sessionRandomExtractDAO = DAOFactory.getSessionRandomExtractDAO();
        int index = 0;
        while (iterator.hasNext()){
          String sessionAggrInfo = iterator.next();
          if (extractIndexList.contains(index)){
            String sessionid = StringUtils.getFieldFromConcatString(
              sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID);

            // 将数据写入MySQL
            SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
            sessionRandomExtract.setTaskid(taskid);
            sessionRandomExtract.setSessionid(sessionid);
            sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(
              sessionAggrInfo, "\\|", Constants.FIELD_START_TIME));
            sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(
              sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
            sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(
              sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));

            sessionRandomExtractDAO.insert(sessionRandomExtract);

            // 将sessionid加入list
            extractSessionIds.add(new Tuple2<String, String>(sessionid, sessionid));
          }
          index++;
        }
        return extractSessionIds;
      }

    });

    /**
     * 第四步：获取抽取出来的session的明细数据
     */
    JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD =
      extractSessionidsRDD.join(sessionid2actionRDD);
    extractSessionDetailRDD.foreach(new VoidFunction<Tuple2<String,Tuple2<String,Row>>>() {

      private static final long serialVersionUID = 1L;

      @Override
      public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
        Row row = tuple._2._2;

        SessionDetail sessionDetail = new SessionDetail();
        sessionDetail.setTaskid(taskid);
        sessionDetail.setUserid(row.getLong(1));
        sessionDetail.setSessionid(row.getString(2));
        sessionDetail.setPageid(row.getLong(3));
        sessionDetail.setActionTime(row.getString(4));
        sessionDetail.setSearchKeyword(row.getString(5));
        sessionDetail.setClickCategoryId(row.getLong(6));
        sessionDetail.setClickProductId(row.getLong(7));
        sessionDetail.setOrderCategoryIds(row.getString(8));
        sessionDetail.setOrderProductIds(row.getString(9));
        sessionDetail.setPayCategoryIds(row.getString(10));
        sessionDetail.setPayProductIds(row.getString(11));

        ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
        sessionDetailDAO.insert(sessionDetail);
      }
    });

  }

  /**
   * 计算各session范围占比，并写入MySQL
   * @param value
   */
  private static void calculateAndPersistAggrStat(String value, long taskid) {
    // 从Accumulator统计串中获取值
    long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(
            value, "\\|", Constants.SESSION_COUNT));

    long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(
            value, "\\|", Constants.TIME_PERIOD_1s_3s));
    long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(
            value, "\\|", Constants.TIME_PERIOD_4s_6s));
    long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(
            value, "\\|", Constants.TIME_PERIOD_7s_9s));
    long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(
            value, "\\|", Constants.TIME_PERIOD_10s_30s));
    long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(
            value, "\\|", Constants.TIME_PERIOD_30s_60s));
    long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(
            value, "\\|", Constants.TIME_PERIOD_1m_3m));
    long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(
            value, "\\|", Constants.TIME_PERIOD_3m_10m));
    long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
            value, "\\|", Constants.TIME_PERIOD_10m_30m));
    long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
            value, "\\|", Constants.TIME_PERIOD_30m));

    long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(
            value, "\\|", Constants.STEP_PERIOD_1_3));
    long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(
            value, "\\|", Constants.STEP_PERIOD_4_6));
    long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(
            value, "\\|", Constants.STEP_PERIOD_7_9));
    long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(
            value, "\\|", Constants.STEP_PERIOD_10_30));
    long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
            value, "\\|", Constants.STEP_PERIOD_30_60));
    long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
            value, "\\|", Constants.STEP_PERIOD_60));

    // 计算各个访问时长和访问步长的范围
    double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
            (double)visit_length_1s_3s / (double)session_count, 2);
    double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
            (double)visit_length_4s_6s / (double)session_count, 2);
    double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
            (double)visit_length_7s_9s / (double)session_count, 2);
    double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
            (double)visit_length_10s_30s / (double)session_count, 2);
    double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
            (double)visit_length_30s_60s / (double)session_count, 2);
    double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
            (double)visit_length_1m_3m / (double)session_count, 2);
    double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
            (double)visit_length_3m_10m / (double)session_count, 2);
    double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
            (double)visit_length_10m_30m / (double)session_count, 2);
    double visit_length_30m_ratio = NumberUtils.formatDouble(
            (double)visit_length_30m / (double)session_count, 2);

    double step_length_1_3_ratio = NumberUtils.formatDouble(
            (double)step_length_1_3 / (double)session_count, 2);
    double step_length_4_6_ratio = NumberUtils.formatDouble(
            (double)step_length_4_6 / (double)session_count, 2);
    double step_length_7_9_ratio = NumberUtils.formatDouble(
            (double)step_length_7_9 / (double)session_count, 2);
    double step_length_10_30_ratio = NumberUtils.formatDouble(
            (double)step_length_10_30 / (double)session_count, 2);
    double step_length_30_60_ratio = NumberUtils.formatDouble(
            (double)step_length_30_60 / (double)session_count, 2);
    double step_length_60_ratio = NumberUtils.formatDouble(
            (double)step_length_60 / (double)session_count, 2);

    // 将统计结果封装为Domain对象
    SessionAggrStat sessionAggrStat = new SessionAggrStat();
    sessionAggrStat.setTaskid(taskid);
    sessionAggrStat.setSession_count(session_count);
    sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
    sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
    sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
    sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
    sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
    sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
    sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
    sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
    sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
    sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
    sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
    sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
    sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
    sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
    sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

    // 调用对应的DAO插入统计结果
    ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
    sessionAggrStatDAO.insert(sessionAggrStat);
  }



  private static JavaPairRDD<String,String> filterSession(JavaPairRDD<String, String> session2AggrInfoRDD, JSONObject taskParam, Accumulator<String> sessionAggrStatAccumulator){
    String stargAge = ParamUtils.getParam(taskParam,Constants.PARAM_START_AGE);
    String endAge = ParamUtils.getParam(taskParam,Constants.PARAM_END_AGE);
    String professionals = ParamUtils.getParam(taskParam,Constants.PARAM_PROFESSIONALS);
    String cities = ParamUtils.getParam(taskParam,Constants.PARAM_CITIES);
    String sex = ParamUtils.getParam(taskParam,Constants.PARAM_SEX);
    String keyWords = ParamUtils.getParam(taskParam,Constants.PARAM_KEYWORDS);
    String categoryIds = ParamUtils.getParam(taskParam,Constants.FIELD_CATEGORY_ID);

    String _parameter = (stargAge != null ? Constants.PARAM_START_AGE + "=" + stargAge + "|" : "")
      + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
      + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
      + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
      + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
      + (keyWords != null ? Constants.PARAM_KEYWORDS + "=" + keyWords + "|" : "")
      + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds: "");
    if(_parameter.endsWith("\\|")) {
      _parameter = _parameter.substring(0, _parameter.length() - 1);
    }

    final String parameter = _parameter;
    JavaPairRDD<String,String> filteredSessionId2AggrRDD =  session2AggrInfoRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
      @Override
      public Boolean call(Tuple2<String, String> tuple) throws Exception {
        String aggrInfo = tuple._2;

        if (!ValidUtils.between(aggrInfo,Constants.FIELD_AGE,parameter,Constants.PARAM_START_AGE,Constants.PARAM_END_AGE)){
          return  false;
        }

        if (!ValidUtils.in(aggrInfo,Constants.FIELD_PROFESSIONAL,parameter,Constants.PARAM_PROFESSIONALS)){
          return false;
        }
        // 按照城市范围进行过滤（cities）
        // 北京,上海,广州,深圳
        // 成都
        if(!ValidUtils.in(aggrInfo, Constants.FIELD_CITY,
          parameter, Constants.PARAM_CITIES)) {
          return false;
        }

        // 按照性别进行过滤
        // 男/女
        // 男，女
        if(!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX,
          parameter, Constants.PARAM_SEX)) {
          return false;
        }

        // 按照搜索词进行过滤
        // 我们的session可能搜索了 火锅,蛋糕,烧烤
        // 我们的筛选条件可能是 火锅,串串香,iphone手机
        // 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
        // 任何一个搜索词相当，即通过
        if(!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS,
          parameter, Constants.PARAM_KEYWORDS)) {
          return false;
        }

        // 按照点击品类id进行过滤
        if(!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS,
          parameter, Constants.PARAM_CATEGORY_IDS)) {
          return false;
        }

        // 如果经过了之前的多个过滤条件之后，程序能够走到这里
        // 那么就说明，该session是通过了用户指定的筛选条件的，也就是需要保留的session
        // 那么就要对session的访问时长和访问步长，进行统计，根据session对应的范围
        // 进行相应的累加计数

        // 主要走到这一步，那么就是需要计数的session
        sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);
        // 计算出session的访问时长和访问步长的范围，并进行相应的累加
        long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo,"\\|",Constants.FIELD_VISIT_LENGTH));
        long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo,"\\|",Constants.FIELD_STEP_LENGTH));
        calculateVisitLength(visitLength);
        calculateStepLength(stepLength);

        return true;
      }

      /**
       * 计算访问时长范围
       * @param visitLength
       */
      private void calculateVisitLength(long visitLength){
        if (visitLength >= 1 && visitLength <= 3){
          sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
        }else if (visitLength >= 4 && visitLength <=6){
          sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
        } else if (visitLength >= 7 && visitLength <= 9){
          sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
        }else if (visitLength >= 10 && visitLength <= 30){
          sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
        }else if(visitLength > 30 && visitLength <= 60) {
          sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
        } else if(visitLength > 60 && visitLength <= 180) {
          sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
        } else if(visitLength > 180 && visitLength <= 600) {
          sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
        } else if(visitLength > 600 && visitLength <= 1800) {
          sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
        } else if(visitLength > 1800) {
          sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
        }
      }

      /**
       * 计算访问步长范围
       * @param stepLength
       */
      private void calculateStepLength(long stepLength){
        if(stepLength >= 1 && stepLength <= 3) {
          sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
        } else if(stepLength >= 4 && stepLength <= 6) {
          sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
        } else if(stepLength >= 7 && stepLength <= 9) {
          sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
        } else if(stepLength >= 10 && stepLength <= 30) {
          sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
        } else if(stepLength > 30 && stepLength <= 60) {
          sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
        } else if(stepLength > 60) {
          sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
        }
      }


    });

    return filteredSessionId2AggrRDD;
  }




  private static JavaPairRDD<String,String> aggregateBySession(JavaRDD<Row> actionRDD,SQLContext sqlContext){
      //现在actionRDD中的元素是Row 一个Row 就是一行用户访问记录 比如一次点击或者搜索
      //我们现在需要将这个row映射成<sessionId,Row>的格式
    JavaPairRDD<String,Row> sessionId2ActionRDD = actionRDD.mapToPair(
      new PairFunction<Row, String, Row>() {

        @Override
        public Tuple2<String, Row> call(Row row) throws Exception {
          return new Tuple2<String, Row>(row.getString(2),row);
        }
      }
    );
    //对行为数据按session粒度进行分组
    JavaPairRDD<String,Iterable<Row>> sessionId2ActionsRDD =
      sessionId2ActionRDD.groupByKey();

    JavaPairRDD<Long,String> userId2PartAggrInfoRDD = sessionId2ActionsRDD.mapToPair(new PairFunction<Tuple2<String,Iterable<Row>>, Long, String>() {

      @Override
      public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
        String seesionId = tuple._1;
        Iterator<Row> iterator = tuple._2.iterator();
        StringBuffer searchKeyWordsBuffer = new StringBuffer();
        StringBuffer clickCategoryIdsBuffer = new StringBuffer();
        Long userId = null;

        int stepLength = 0;
        Date startDateTime = null;
        Date endDateTime = null;

        //遍历session所有访问行为
        while (iterator.hasNext()){
          //提取每个访问行为的搜索字段和点击品类字段
          Row row = iterator.next();
          if (userId == null){
            userId = row.getLong(1);
          }
          String searchKeyWord = row.getString(5);
          Long clickCategoryId = row.getLong(6);

          if (StringUtils.isNotEmpty(searchKeyWord)){
            if (!searchKeyWordsBuffer.toString().contains(searchKeyWord)){
              searchKeyWordsBuffer.append(searchKeyWord).append(",");
            }
          }
          if (clickCategoryId != null){
            if (!clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))){
              clickCategoryIdsBuffer.append(clickCategoryId).append(",");
            }
          }

          Date actionTime = DateUtils.parseTime(row.getString(4));

          if (startDateTime == null){
            startDateTime = actionTime ;
          }

          if (endDateTime == null){
            endDateTime = actionTime ;
          }

          if (actionTime.before(startDateTime)){
            startDateTime = actionTime;
          }

          if (actionTime.after(endDateTime)){
            endDateTime  = actionTime;
          }

          stepLength++;
        }

        Long visit_time_length = ( endDateTime.getTime() - startDateTime.getTime() ) / 1000;


        String searchKeyWords = StringUtils.trimComma(searchKeyWordsBuffer.toString());
        String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());
        String partAggr = Constants.FIELD_SESSION_ID + "=" + seesionId + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeyWords + "|" +
          Constants.FIELD_CATEGORY_ID +"=" + clickCategoryIds + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visit_time_length + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startDateTime);

        return new Tuple2<Long, String>(userId,partAggr);
      }
    });

    String sql = "select * from user_info";
    JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();

    JavaPairRDD<Long,Row> userId2InfoRDD = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
      @Override
      public Tuple2<Long, Row> call(Row row) throws Exception {
        return new Tuple2<Long, Row>(row.getLong(0),row);
      }
    });

    JavaPairRDD<Long,Tuple2<String,Row>> userId2FullInfoRDD = userId2PartAggrInfoRDD.join(userId2InfoRDD);

    JavaPairRDD<String,String>  sessionId2FullAggrInfoRDD = userId2FullInfoRDD.mapToPair(new PairFunction<Tuple2<Long,Tuple2<String,Row>>, String, String>() {
      @Override
      public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
        String partAggrInfo = tuple._2._1;
        Row userInfoRow = tuple._2._2;
        String sessionId = StringUtils.getFieldFromConcatString(partAggrInfo,"\\|",Constants.FIELD_SESSION_ID);
        int age = userInfoRow.getInt(3);
        String professional = userInfoRow.getString(4);
        String city = userInfoRow.getString(5);
        String sex = userInfoRow.getString(6);

        String fullAggrInfo = partAggrInfo + "|" + Constants.FIELD_AGE + "=" + age
          + "|" + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
          + Constants.FIELD_CITY +"=" + city + "|"
          + Constants.FIELD_SEX + "=" + sex;
        return new Tuple2<String, String>(sessionId,fullAggrInfo);
      }
    });

    return sessionId2FullAggrInfoRDD;
  }

  /**
   * 获取指定日期范围内的用户访问数据
   * @param sqlContext
   * @param taskParam 任务参数
   * @return 行为数据RDD
     */
  private static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext,JSONObject taskParam){
    String startDate = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE);
    String endDate = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE);

    String sql = "select * from user_visit_action where date >='"
      + startDate +"'  and date <='" + endDate + "'";

    DataFrame actionDF = sqlContext.sql(sql);

    return actionDF.javaRDD();
  }




  /**
   * 获取SQLContext
   * 如果是在本地测试环境的话，那么就生成SQLContext对象
   * 如果是在生产环境运行的话，那么就生成HiveContext对象
   * @param sc SparkContext
   * @return SQLContext
   */
  private static SQLContext getSQLContext(SparkContext sc) {
    boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
    if(local) {
      return new SQLContext(sc);
    } else {
      return new HiveContext(sc);
    }
  }


  /**
   * 生成模拟数据（只有本地模式，才会去生成模拟数据）
   * @param sc
   * @param sqlContext
   */
  private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
    boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
    if(local) {
      MockData.mock(sc, sqlContext);
    }
  }


}
