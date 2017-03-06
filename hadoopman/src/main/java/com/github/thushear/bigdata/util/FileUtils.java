package com.github.thushear.bigdata.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by kongming on 2017/1/9.
 */
public class FileUtils {

    private static AtomicLong atomicLong = new AtomicLong();

    static String PATH = "/root/mapreduce.log";

    private static File DEFAULT_FILE = new File(PATH);

//    static {
////     if (!DEFAULT_FILE.exists()){
////       DEFAULT_FILE.mkdir();
////     }
//
////    try {
////      FileSystem fileSystem =  getDFSFileSystem();
////      fileSystem.create(new Path(PATH),true);
////    } catch (IOException e) {
////      e.printStackTrace();
////    }
//    }


    public static void writeToFile(String data, boolean append) {
        FSDataOutputStream fsDataOutputStream = null;
        HTable logsTable = null;
        try {
            System.err.println(data);
            org.apache.commons.io.FileUtils.write(DEFAULT_FILE, data + "\n", append);
            Path writePath = new Path(PATH);
            FileSystem fileSystem = getDFSFileSystem();
            fsDataOutputStream = fileSystem.append(writePath);
            fsDataOutputStream.writeUTF(data);
            fsDataOutputStream.flush();

            logsTable = getHTableByName("logs");
            hPut(logsTable, data, "b", "logs");
        } catch (IOException e) {
            e.printStackTrace();

        } finally {
            IOUtils.closeStream(logsTable);
//      IOUtils.closeStream(fsDataOutputStream);
        }
    }


    /**
     * 写入
     *
     * @param hTable
     * @throws InterruptedIOException
     * @throws RetriesExhaustedWithDetailsException
     */
    private static void hPut(HTable hTable, String data, String columnFamily, String column) throws InterruptedIOException, RetriesExhaustedWithDetailsException {
        Put put = new Put(Bytes.toBytes(String.valueOf(System.currentTimeMillis() + "|" + atomicLong.incrementAndGet())));
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
        hTable.put(put);
    }

    /**
     * 获取HTable
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    private static HTable getHTableByName(String tableName) throws IOException {
        Configuration configuration = HBaseConfiguration.create();

        return new HTable(configuration, tableName);
    }


    public static void writeToFile(File file, CharSequence data, boolean append) {

        try {
            org.apache.commons.io.FileUtils.write(file, data + "\n", append);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 获取HDFS文件系统
     *
     * @return
     * @throws IOException
     */
    private static FileSystem getDFSFileSystem() throws IOException {

        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);

        return fileSystem;
    }

    public static void main(String[] args) throws IOException {

        Date date = new Date(1449917532123l);
        System.out.println("date = " + date);
        FSDataOutputStream fsDataOutputStream = null;
        Path writePath = new Path(PATH);
        FileSystem fileSystem = getDFSFileSystem();
        fsDataOutputStream = fileSystem.append(writePath);
        fsDataOutputStream.writeUTF("mnnnns\n");

        fsDataOutputStream.flush();


    }


}
