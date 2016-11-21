package com.github.thushear.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InterruptedIOException;

/**
 * Created by kongming on 2016/11/19.
 */
public class HBaseMan {


    public static void main(String[] args) throws IOException {

      String tableName = "user";

      HTable hTable = getHTableByName(tableName);

      hget(hTable);

     // hPut(hTable);

     // hDelete(hTable);

      scan(hTable);

      IOUtils.closeStream(hTable);

    }

  private static void scan(HTable hTable) throws IOException {
    Scan scan = new Scan();
    //startRow stopRow 包头不包尾

    scan.setStartRow(Bytes.toBytes("10002"));
    scan.setStopRow(Bytes.toBytes("10004"));

    PrefixFilter prefixFilter = new PrefixFilter(Bytes.toBytes("200"));
//    PageFilter pageFilter = new PageFilter();
    scan.setFilter(prefixFilter);

    ResultScanner resultScanner = hTable.getScanner(scan);

    for (Result result : resultScanner) {
      System.out.println(Bytes.toString(result.getRow()));
      Cell[] cells = result.rawCells();

      for (Cell cell : cells) {
        System.out.println( Bytes.toString(CellUtil.cloneRow(cell))  + "-->" + Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
          + Bytes.toString(CellUtil.cloneQualifier(cell)) + "-->" + Bytes.toString(CellUtil.cloneValue(cell)));
      }
      System.out.println("--------------------------------------");
    }

    IOUtils.closeStream(resultScanner);
  }

  /**
   * 删除
   * @param hTable
   * @throws IOException
     */
  private static void hDelete(HTable hTable) throws IOException {
    Delete delete = new Delete(Bytes.toBytes("10001"));

//      delete.deleteColumn(Bytes.toBytes("info"),Bytes.toBytes("name"));
    //无法删除某一行 只能通过挨个删除 columnfamily的方式
    delete.deleteFamily(Bytes.toBytes("info"));

    hTable.delete(delete);
  }


  /**
   * 写入
   * @param hTable
   * @throws InterruptedIOException
   * @throws RetriesExhaustedWithDetailsException
     */
  private static void hPut(HTable hTable) throws InterruptedIOException, RetriesExhaustedWithDetailsException {
    Put put = new Put(Bytes.toBytes("10003"));
    put.add(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes("zhangsan"));
    put.add(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(22));
    put.add(Bytes.toBytes("info"),Bytes.toBytes("sex"),Bytes.toBytes("male"));
    put.add(Bytes.toBytes("info"),Bytes.toBytes("address"),Bytes.toBytes("北京"));
    hTable.put(put);
  }


  /**
   * 读取
   * @param hTable
   * @throws IOException
     */
  private static void hget(HTable hTable) throws IOException {
    Get get = new Get(Bytes.toBytes("10001"));

 //   get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"));

    Result result = hTable.get(get);

    Cell[] cells = result.rawCells();

    for (Cell cell : cells) {
      System.out.println( Bytes.toString(CellUtil.cloneRow(cell))  + "-->" + Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
        + Bytes.toString(CellUtil.cloneQualifier(cell)) + "-->" + Bytes.toString(CellUtil.cloneValue(cell)));
    }
  }

  /**
   * 获取HTable
   * @param tableName
   * @return
   * @throws IOException
     */
  private static HTable getHTableByName(String tableName) throws IOException {
    Configuration configuration = HBaseConfiguration.create();

    return new HTable(configuration,tableName);
  }


}
