package com.github.thushear.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;

/**
 * Created by kongming on 2016/10/24.
 */
public class HdfsApiMan {


  public static void main(String[] args) throws IOException {
    String fileName = "/user/testfile.txt";
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

//    System.out.println("fileSystem = " + fileSystem);

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
