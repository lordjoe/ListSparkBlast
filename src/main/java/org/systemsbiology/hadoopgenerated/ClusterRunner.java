package org.systemsbiology.hadoopgenerated;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

import java.io.*;


/**
 * org.systemsbiology.hadoopgenerated.ClusterRunner
 * User: steven
 * Date: 12/10/12
 */
public class ClusterRunner {
    public static final ClusterRunner[] EMPTY_ARRAY = {};

    public static final String CHANGE_THIS = "CHANGE_THIS";

//    public static final String HDFS_HOST = CHANGE_THIS; //"hdfs://MyServer:9000";
//    public static final String JOB_TRACKER = CHANGE_THIS; // "glados:9001";
//    public static final String JAR_FILE = CHANGE_THIS; // "NShot.jar"";

    public static final String HDFS_HOST = "hdfs://glados:9000";
    public static final String JOB_TRACKER = "glados:9001";
    public static final String JAR_FILE =  "NShot.jar";

    private static Configuration buildConfiguration(String jarFile) {
        Configuration conf = new Configuration();
        conf.set("mapred.jar", jarFile);
        //     }


        conf.set("fs.default.name", HDFS_HOST);
        conf.set("mapred.job.tracker", JOB_TRACKER);

        String maxMamory = "1024";

        // set twice max memory
        Long.toString((Integer.parseInt(maxMamory) * 2048) + 200 * 1024);
        conf.set("mapred.child.ulimit", Long.toString((Integer.parseInt(maxMamory) * 2048) + 200 * 1024));  // in kb
        // DO NOT - DO NOT SET   -xx:-UseGCOverheadLimit   leads to error - Error reading task outputhttp://glad
        conf.set("mapred.child.java.opts", "-Xmx" + maxMamory + "m" + " "
                + "-Djava.net.preferIPv4Stack=true"
        ); // NEVER DO THIS!!!! +   " -xx:-UseGCOverheadLimit");
        return conf;
    }

    public static void main(String[] args) throws Exception {
        if (CHANGE_THIS.equals(HDFS_HOST))
            throw new IllegalStateException("make HDFS_HOST something like hdfs://MyServer:9000"); // ToDo change
        if (CHANGE_THIS.equals(JOB_TRACKER))
            throw new IllegalStateException("make HDFS_HOST something like MyServer:9001"); // ToDo change
        if (CHANGE_THIS.equals(JAR_FILE))
            throw new IllegalStateException("make jarfile comething like /users/home/mydata.NShot.jar"); // ToDo change

        String jarFile = "NShot.jar";
        File file = new File(jarFile);
        if (!file.exists())
            throw new IllegalStateException("jar file  " + file.getAbsolutePath() + " does not exist");

        Configuration conf = buildConfiguration(jarFile);

          Tool tool = new HadoopTest();
    //    Tool tool = new NShotTest();
         tool.setConf(conf);
        int ans = tool.run(args);

    }


}
