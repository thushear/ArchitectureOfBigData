package com.github.thushear.spark.spark.session;

/**
 * Created by kongming on 2017/1/8.
 */

import com.alibaba.fastjson.JSONObject;
import com.github.thushear.spark.conf.ConfigurationManager;
import com.github.thushear.spark.constant.Constants;
import com.github.thushear.spark.dao.ITaskDAO;
import com.github.thushear.spark.dao.factory.DAOFactory;
import com.github.thushear.spark.domain.Task;
import com.github.thushear.spark.test.MockData;
import com.github.thushear.spark.util.ParamUtils;
import com.github.thushear.spark.util.StringUtils;
import com.github.thushear.spark.util.ValidUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.Date;
import java.util.Iterator;
import java.util.List;

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
    JavaPairRDD<String,String> sessionId2FullAggrInfoRDD = aggregateBySession(actionRDD,sqlContext);
    System.out.println("sessionId2FullAggrInfoRDD=" + sessionId2FullAggrInfoRDD.count());
    List<Tuple2<String,String>>  list = sessionId2FullAggrInfoRDD.take(10);
    for (Tuple2<String, String> tuple2 : list) {
      System.out.println(tuple2._1 + ":" + tuple2._2);
    }

    JavaPairRDD<String,String> filteredSessionId2AggrRDD = filterSession(sessionId2FullAggrInfoRDD,taskParam);

    System.out.println("filteredSessionId2AggrRDD= " + filteredSessionId2AggrRDD.count());
    for (Tuple2<String, String> tuple2 : filteredSessionId2AggrRDD.take(10)) {
      System.out.println(tuple2._1 + ":" + tuple2._2);
    }

    sc.close();

  }

  private static JavaPairRDD<String,String> filterSession(JavaPairRDD<String,String> session2AggrInfoRDD,JSONObject taskParam){
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



        return true;
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
        }

        String searchKeyWords = StringUtils.trimComma(searchKeyWordsBuffer.toString());
        String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());
        String partAggr = Constants.FIELD_SESSION_ID + "=" + seesionId + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeyWords + "|" +
          Constants.FIELD_CATEGORY_ID +"=" + clickCategoryIds;

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
