package com.github.thushear.storm.utils;

import org.apache.storm.shade.org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

/**
 * Created by kongming on 2016/12/11.
 */
public class MockData {

  public static List<String>  mockLineList ;

  static {

    try {
      mockLineList = IOUtils.readLines(new FileReader(new File(MockData.class.getResource("/").getPath() + File.separator + "access.log")));
    } catch (IOException e) {
      e.printStackTrace();
    }

  }





}
