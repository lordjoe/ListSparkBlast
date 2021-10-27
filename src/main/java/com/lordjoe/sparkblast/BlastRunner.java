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
import org.apache.spark.storage.StorageLevel;
import scala.Option;
import scala.Tuple2;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Random;


/**
 * com.lordjoe.sparkblast.BlastRunner
 * User: Steve
 * Date: 6/21/2021
 */
public class BlastRunner {
    public static final Random RND = new Random();

    public static void main(String[] args) throws Exception {
        SparkConf conf2 = new SparkConf();

        Option<String> option = conf2.getOption("spark.master");
        if ( !option.isDefined()) {   // use local over nothing
            conf2.setMaster("local[*]");
        }
        conf2.setAppName("Spark Blast");
         JavaSparkContext sc = new JavaSparkContext(conf2);
        int splits = Integer.parseInt(args[0]);
        splits = Math.max(1,splits);
        Configuration conf = sc.hadoopConfiguration();

        conf.set("org.systemsbiology.jxtandem.DesiredDatabaseMappers",Integer.toString(splits));
        /* Set delimiter to split file in correct local*/
        conf.set("textinputformat.record.delimiter", ">") ;
        String script = args[1];
        String filename = args[2];
        String outfile = args[3];
        File test = new File(outfile);

        /**
         * make a map from Header to index
         */
        File fasta = new File(filename);
        long fileLength = fasta.length();
        conf.setLong("org.systemsbiology.jxtandem.DesiredDatabaseMappers", splits);
        conf.setLong("mapreduce.input.fileinputformat.split.maxsize", fileLength/ splits);
        final Map<String,Integer> ordering = HeaderOrder.readHeaderOrder(fasta);


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

        JavaRDD<String> repartitionDataset = parte4;
        /* Option to repartition file in splits defined on 'val splits'*/
      //  JavaRDD<String> repartitionDataset = parte4.repartition(splits );
//        repartitionDataset = Utilities.view(sc,repartitionDataset );

        JavaRDD<String> pipe = repartitionDataset.pipe(script);

     //   pipe = Utilities.view(sc,pipe );
//        pipe = pipe.persist(StorageLevel.MEMORY_AND_DISK());
//        String header = findHeader(pipe.first()) ;
//
//        JavaPairRDD<Integer,String>  identifiedFinds  = pipe.flatMapToPair(new PairFlatMapFunction<String, Integer, String>() {
//            @Override
//            public Iterable<Tuple2<Integer, String>> call(String s) throws Exception {
//                List<Tuple2<Integer, String>> ret = new ArrayList<Tuple2<Integer, String>>();
//                StringBuilder sb = new StringBuilder();
//                String query = null;
//                String[] lines = s.split("\n") ;
//                for (int i = 0; i < lines.length; i++) {
//                    String line = lines[i];
//
//                }
//                return ret;
//            }
//        }) ;
//        JavaPairRDD<Integer, String> integerStringJavaPairRDD = identifiedFinds.sortByKey();

        MapQueries operator = new MapQueries(ordering);

        JavaRDD<String> persist = pipe.persist(StorageLevel.MEMORY_AND_DISK());
        String tempName = Integer.toString(Math.abs(RND.nextInt())) ;

        JavaPairRDD<String, String> headAndFoot = persist.flatMapToPair(new GetHeaderAndFooter());

        List<Tuple2<String, String>> collect1 = headAndFoot.collect();
        String footer = "";
        String header = "";
        for (Tuple2<String, String> ts : collect1) {
            String id = ts._1;
            String value = ts._2;
            if(id.equals(GetHeaderAndFooter.Footer))
                footer = value;
            if(id.equals(GetHeaderAndFooter.Header))
                header = value;
        }

        persist = persist.coalesce(1); //we need to handle this on one machine
        JavaPairRDD<Integer, String> queries = persist.flatMapToPair(operator);
   //     List<Tuple2<Integer, String>> collect1 = queries.collect();

        queries = queries.persist(StorageLevel.MEMORY_AND_DISK());

  //      List<Tuple2<Integer, String>> collect1 = queries.collect();
        
        JavaPairRDD<Integer, String> sorted = queries.sortByKey();
  //       sorted = sorted.persist(StorageLevel.MEMORY_AND_DISK());
  //      List<Tuple2<Integer, String>> collect2 = sorted.collect();

        JavaPairRDD<Integer, String> writer = sorted.coalesce(1);
  //      writer = writer.persist(StorageLevel.MEMORY_AND_DISK());
  //      List<Tuple2<Integer, String>> collect3 = writer.collect();

        BlastWriter outMap = new BlastWriter(outfile,header,ordering.size());

        JavaRDD<Object> map = writer.map(outMap);
        map.collect(); // force map to happen

        PrintWriter pw = new PrintWriter(new FileWriter(outfile,true)) ;
        pw.println(footer);
        pw.close();

        sc.stop();
     }

    private static void writeTempFile(String tempName, List<String> collect) {
        try {
            boolean inHeader = true;
            boolean inFooter = false;

            File f = new File(tempName + ".header");
            File f2 = new File(tempName + ".footer");
            PrintWriter pw = new PrintWriter(new FileWriter(f)) ;
            for (String line : collect) {
                if (inHeader) {
                    if (line.startsWith("Query=")) {
                        inHeader = false;
                        pw.close();
                    }
                    else {
                        pw.println(line);
                    }
                 }
                else {
                    if (inFooter) {
                        if (line.startsWith("BLAST")) {
                            pw.close();
                            return;
                        }
                        else {
                            pw.println(line);
                        }
                    }
                    else {
                        if (line.contains("Database:")) {
                            pw = new PrintWriter(new FileWriter(f2));
                            inFooter = true;
                            pw.println(line);
                        }
 
                    }
                 }
             }
            pw.close();
        }


        catch (IOException e) {
            throw new RuntimeException(e);

        }
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
