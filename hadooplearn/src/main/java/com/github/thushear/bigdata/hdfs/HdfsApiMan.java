package com.github.thushear.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

/**
 * Created by kongming on 2016/10/24.
 */
public class HdfsApiMan {


  public static void main(String[] args) throws IOException {

    FileSystem fileSystem = getDFSFileSystem();

    System.out.println("fileSystem = " + fileSystem);

  }

  private static FileSystem getDFSFileSystem() throws IOException {

    Configuration configuration = new Configuration();
    FileSystem fileSystem = FileSystem.get(configuration);

    return fileSystem;
  }


}
