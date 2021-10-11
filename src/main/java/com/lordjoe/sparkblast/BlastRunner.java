package com.lordjoe.sparkblast;

import com.lordjoe.sparkutilities.Utilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Option;
import scala.Tuple2;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * com.lordjoe.sparkblast.BlastRunner
 * User: Steve
 * Date: 6/21/2021
 */
public class BlastRunner {
    public static final BlastRunner[] EMPTY_ARRAY = {};

    public static void main(String[] args) {
        SparkConf conf2 = new SparkConf();

        Option<String> option = conf2.getOption("spark.master");
        if ( !option.isDefined()) {   // use local over nothing
            conf2.setMaster("local[*]");
        }
        conf2.setAppName("Spark Blast");
         JavaSparkContext sc = new JavaSparkContext(conf2);
        int splits = Integer.parseInt(args[0]);
        Configuration conf = sc.hadoopConfiguration();

        /* Set delimiter to split file in correct local*/
        conf.set("textinputformat.record.delimiter", ">") ;
        String script = args[1];
        String filename = args[2];
        String outfile = args[3];
        File test = new File(outfile);

        /**
         * make a map from Header to index
         */
         final Map<String,Integer> ordering = HeaderOrder.readHeaderOrder(new File(filename));


        //    filename = filename.replace("\\","/");
    //    filename = "file:///" + filename;
    //    Path inputPath = new Path( filename);
//        String path = filename.toString();
//        JavaRDD<String> input = sc.textFile(filename);
//        input = Utilities.view(sc,input );


        JavaPairRDD< LongWritable, Text> dataset =  sc.newAPIHadoopFile(filename,  TextInputFormat.class,  LongWritable.class,  Text.class,conf );

        JavaRDD<String> mapDataset = dataset.map(x -> x._2.toString());
//           mapDataset = Utilities.view(sc,mapDataset );

        /* Replace first character to add simbol '>'*/
        JavaRDD<String> parte2 = mapDataset.map(x->x.replaceFirst("gi|",">gi|"))  ;
    //    parte2 = Utilities.view(sc,parte2 );

        JavaRDD<String> parte3 = parte2.map(x->x.replaceFirst("sp|",">sp|"));
        parte3 = Utilities.view(sc,parte3 );
        JavaRDD<String> parte4 = parte3.map(x->x.replaceFirst("tr|",">tr|"));
  //      parte4 = Utilities.view(sc,parte4 );

        /* Option to repartition file in splits defined on 'val splits'*/
        JavaRDD<String> repartitionDataset = parte4.repartition(splits );
//        repartitionDataset = Utilities.view(sc,repartitionDataset );

        JavaRDD<String> pipe = repartitionDataset.pipe(script);
     //   pipe = Utilities.view(sc,pipe );
        pipe = pipe.persist(StorageLevel.MEMORY_AND_DISK());
        String header = findHeader(pipe.first()) ;

        JavaPairRDD<Integer,String>  identifiedFinds  = pipe.flatMapToPair(new PairFlatMapFunction<String, Integer, String>() {
            @Override
            public Iterable<Tuple2<Integer, String>> call(String s) throws Exception {
                List<Tuple2<Integer, String>> ret = new ArrayList<Tuple2<Integer, String>>();
                StringBuilder sb = new StringBuilder();
                String query = null;
                String[] lines = s.split("\n") ;
                for (int i = 0; i < lines.length; i++) {
                    String line = lines[i];

                }
                return ret;
            }
        }) ;
        JavaPairRDD<Integer, String> integerStringJavaPairRDD = identifiedFinds.sortByKey();

        pipe.saveAsTextFile(args[3]);
        sc.stop();

    }

    private static String findHeader(String first) {
        StringBuilder sb = new StringBuilder();
        int index = 0;
        for (int i = 0; i < first.length(); i++) {
            char c = first.charAt(index++);

            if(c == '\n')  {
                c = first.charAt(index++);
                if(c == '\r') {
                    c = first.charAt(index++);
                }
                if(c == '>')
                    return sb.toString();
                 else {
                    sb.append('\n') ;
                    sb.append(c) ;
                 }
              }
             else {
                sb.append(c) ;

            }
        }
         return sb.toString();
    }
}
