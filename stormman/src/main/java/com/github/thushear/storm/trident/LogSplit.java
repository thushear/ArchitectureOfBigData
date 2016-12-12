package com.github.thushear.storm.trident;

import com.github.thushear.storm.utils.LogFormatter;
import com.google.common.base.Strings;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * Created by kongming on 2016/12/11.
 */
public class LogSplit extends BaseFunction {

  static String orginalDatePattern = "dd/MMM/yyyy:HH:mm:ss";

  static  String datePattern = "yyyy-MM-dd";

  @Override
  public void execute(TridentTuple tuple, TridentCollector collector) {
    String log = tuple.getString(0);
    if (log != null) {
      String logArray[] = log.split(" ");

      SimpleDateFormat originalFormat = new SimpleDateFormat(orginalDatePattern, Locale.ENGLISH);
      SimpleDateFormat dateFormat = new SimpleDateFormat(datePattern);
      try {
        String parseDate = StringUtils.substring(logArray[3],1);
        Date date = originalFormat.parse(parseDate);
        String dateStr = dateFormat.format(date);
        LogFormatter.trace("Class LogSplit Method execute ThreadId %s ip %s dateStr %s",
          Thread.currentThread().getId(),logArray[0],dateStr);
        collector.emit(new Values(logArray[0],dateStr));
      } catch (ParseException e) {
        e.printStackTrace();
      }


    }


  }


  public static void main(String[] args) {
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(orginalDatePattern, Locale.ENGLISH);
    String foramt = simpleDateFormat.format(new Date());
    LogFormatter.trace("format %s",foramt);
  }
}
