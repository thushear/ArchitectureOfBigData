package com.github.thushear.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by kongming on 2016/10/24.
 */
public class HdfsApiMan {


  public static void main(String[] args) throws IOException {
//    String fileName = "/file/testfile.txt";
//    readData(fileName);
    String putFilePath = "/file/write.txt";
//    writeDataToDFS(putFilePath);
    readData(putFilePath);
//    System.out.println("fileSystem = " + fileSystem);

  }


  /**
   * 写文件到HDFS
   * @param putFilePath
   * @throws IOException
     */
  private static void writeDataToDFS(String putFilePath) throws IOException {
    Path writePath = new Path(putFilePath);
    FileSystem fileSystem =  getDFSFileSystem();
    FSDataOutputStream fsDataOutputStream = fileSystem.create(writePath);
    InputStream inputStream = HdfsApiMan.class.getResourceAsStream("/log4j.properties");

    try {
      IOUtils.copyBytes(inputStream,fsDataOutputStream,4096,false);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      IOUtils.closeStream(fsDataOutputStream);
      IOUtils.closeStream(inputStream);
    }
  }


  /**
   * 读取文件s
   * @param fileName
   * @throws IOException
     */
  private static void readData(String fileName) throws IOException {
    FileSystem fileSystem = getDFSFileSystem();
    System.out.println("fileSystem = " + fileSystem);
    Path path = new Path(fileName);
    FSDataInputStream fsDataInputStream = fileSystem.open(path);

    try {
      IOUtils.copyBytes(fsDataInputStream, System.out, 4096, false);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      IOUtils.closeStream(fsDataInputStream);
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


}
