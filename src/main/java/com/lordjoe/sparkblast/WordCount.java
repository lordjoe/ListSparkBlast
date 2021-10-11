package com.lordjoe.sparkblast;

import com.lordjoe.sparkutilities.Utilities;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Option;
import scala.Tuple2;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.*;

/**
 * com.lordjoe.sparkblast.WordCount
 * User: Steve
 * Date: 6/29/21
 */
public class WordCount {

    private static String replacePunctuation(String x) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < x.length(); i++) {
             char c = x.charAt(i) ;
             if(Character.isLetter(c) || c == ' ')
                 sb.append(c);
             else
                 sb.append(' ');

        }
        return sb.toString();
    }
    public static void main(String[] args) throws Exception {
        String inputFile = args[0];
        String outputFile = args[1];
        // Create a Java Spark Context.
        SparkConf sparkConf = new SparkConf();

        String test = System.getenv("BLAST_HOME");


        Option<String> option = sparkConf.getOption("spark.master");
        if ( !option.isDefined()) {   // use local over nothing
            sparkConf.setMaster("local[*]");
        }
        SparkConf conf = sparkConf.setAppName("wordCount");
        for (Tuple2<String, String> kv : conf.getAll()) {
            System.out.println(kv._1 + " = " + kv._2);
        }

        JavaSparkContext sc = new JavaSparkContext(conf);
        // Load our input data.
        JavaRDD<String> input = sc.textFile(inputFile);
        input = Utilities.view(sc,input );
        // Split up into words.
        JavaRDD<String> words = input.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterable<String> call(String x) throws Exception {
                        x = replacePunctuation(x);
                        String[] items = x.split(" ");
                        for (int i = 0; i < items.length; i++) {
                            items[i] = items[i].toUpperCase();

                        }
                        return  (Iterable<String>)Arrays.asList(items).iterator();
                    }
                });
        // Transform into word and count.
        JavaPairRDD<String, Integer> counts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String x) {
                        return new Tuple2(x, 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer x, Integer y) {
                return x + y;
            }
        });
        // Save the word count back out to a text file, causing evaluation.
        Map<String, Integer> asMap = counts.collectAsMap();
        List<String> wordsFound = new ArrayList<>(asMap.keySet());
        Collections.sort(wordsFound);
        PrintWriter out = new PrintWriter(new FileWriter(outputFile));
        for (String s : wordsFound) {
            out.println(s + ":" + asMap.get(s));
        }
        out.close();
    }


}
