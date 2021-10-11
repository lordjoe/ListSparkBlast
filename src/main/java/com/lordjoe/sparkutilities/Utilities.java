package com.lordjoe.sparkutilities;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.List;

/**
 * com.lordjoe.sparkutilities.Utilities
 * User: Steve
 * Date: 6/21/2021
 */
public class Utilities {

    /**
     * all a peak at a small RDD
     * @param sc
     * @param in
     * @param <T>
     * @return
     */
    public static <T> JavaRDD<T>  view(JavaSparkContext sc, JavaRDD<T> in) {
        List<T> data = in.collect();
        return  sc.parallelize(data);
    }

    public static String readInFile(File f) {
        StringBuilder sb = new StringBuilder();

        try {
            LineNumberReader rdr = new LineNumberReader(new FileReader(f)) ;
            String line = rdr.readLine();
            while(line != null) {
                sb.append(line);
                sb.append('\n');
                line = rdr.readLine();
            }

            return sb.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);

        }
    }
}
