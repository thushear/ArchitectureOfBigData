package com.github.thushear.storm.utils;


import org.apache.storm.shade.org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Log Trace Util
 * Created by kongming on 2016/12/7.
 */
public final class LogFormatter {

    static FileWriter fileWriter;

    static {
        try {
            fileWriter = new FileWriter(new File(LogFormatter.class.getResource("/").getPath() + File.separator + "trace.log"),true);
        } catch (IOException e) {
            e.printStackTrace();

            IOUtils.closeQuietly(fileWriter);
        }
    }


    public static void trace(String formatter,Object... formatterParams){
        if (formatter != null) {
            formatter.trim();
        }
        try {

            traceToFile(formatter,formatterParams);
        } catch (IOException e) {
            e.printStackTrace();
        }

        traceToStdErr(formatter,formatterParams);

    }


    public static void traceToFile(String formatter,Object... formatterParams) throws IOException {
        fileWriter.append(String.format(formatter,formatterParams));
    }

    public static void traceToStdErr(String formatter,Object... formatterParams){
        System.err.format(formatter,formatterParams);
    }


    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        IOUtils.closeQuietly(fileWriter);
    }
}
