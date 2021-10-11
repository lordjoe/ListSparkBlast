package com.lordjoe.spark;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Option;

import java.util.ArrayList;
import java.util.List;

/**
 * com.lordjoe.spark.NShotRunner
 * spark-submit.cmd --jars \NShotTest\out\artifacts\NShot.jar --class com.lordjoe.spark.NShotRunner IGNORE_WHAT_THE 3 10
 * User: Steve
 * Date: 3/28/2016
 */
public class NShotRunner {

    private static boolean logSetToWarn;
    public static JavaSparkContext setupSparkContext(String name) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName(name);
        // if not on a cluster then run locally
        Option<String> option = sparkConf.getOption("spark.master");
        if (!option.isDefined()) {   // use local over nothing
            sparkConf.setMaster("local[*]");
        }



        return new JavaSparkContext(sparkConf);
    }

    public static void setLogToWarn() {
        if (!logSetToWarn) {
            Logger rootLogger = Logger.getRootLogger();
            rootLogger.setLevel(Level.WARN);

            // not sure why this is needed
            setNamedLoggerToWarn("com.lordjoe");
            setNamedLoggerToWarn("org");
            setNamedLoggerToWarn("spark");
            setNamedLoggerToWarn("akka");
            setNamedLoggerToWarn("sparkDriver-akka");

            logSetToWarn = true;
        }
    }

    protected static void setNamedLoggerToWarn(String name) {
        try {
            Logger spark = Logger.getLogger(name);
            if (spark != null)
                spark.setLevel(Level.WARN);
        } catch (Exception e) {
            // forgive errors
        }

    }
    /**
     * arge are the number of levels and the expansion factor
     *
     * @param args
     */
    public static void main(String[] args) {

        System.out.print("Args: " );
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            System.out.print(" " + arg);
        }
        System.out.println();

        int numberLevels = Integer.parseInt(args[0]);
        int expansionFactor = Integer.parseInt(args[1]);

        System.out.println("numberLevels " + numberLevels + " expansionFactor " + expansionFactor);

        double expected = Math.pow(expansionFactor, numberLevels);

        JavaSparkContext sc = setupSparkContext("NShotRunner");
        setLogToWarn();


        List<NShot> holder = new ArrayList<NShot>();
        holder.add(new NShot(0));

        JavaRDD<NShot> level = sc.parallelize(holder);
        for (int i = 0; i < numberLevels; i++) {
            level = level.flatMap(new NShotMapper(expansionFactor));
        }

        long count = level.count();

        if (Math.abs(expected - count) > 0.001)
            throw new IllegalStateException("not " + expected + " but " + count);

        System.out.println("found " + count);
        System.out.println("Done");


    }



}
