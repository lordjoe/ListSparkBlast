package com.lordjoe.sparkblast;

import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * com.lordjoe.sparkblast.GetHeaderAndFooter
 * User: Steve
 * Date: 10/26/21
 */
public class GetHeaderAndFooter implements PairFlatMapFunction<String, String,String> {
    public static final String Header = "Header";
    public static final String Footer = "Footer";

     private StringBuilder sb = new StringBuilder();
      private boolean headerSeen = false;




    @Override
    public Iterable<Tuple2<String, String>> call(String line) throws Exception {
        List<Tuple2<String, String>> tuple2s = new ArrayList<Tuple2<String, String>>();
        int partitionId = TaskContext.getPartitionId();
        if(partitionId != 0)   // only do this for one partition
             return tuple2s;
        if(!headerSeen) {
            if(line.startsWith("Query=")) {
                headerSeen = true;
                tuple2s.add(new Tuple2(Header, sb.toString()));
                sb.setLength(0);
                return tuple2s;
            }
        }
        if(line.length() > 0 || sb.length() > 0)
            sb.append(line + "\n") ;
        // kill query text
        if(line.startsWith("Effective search space used:"))
            sb.setLength(0);

        if(line.startsWith("Window for multiple hits")) {
            headerSeen = true;
            tuple2s.add(new Tuple2(Footer, sb.toString()));
            sb.setLength(0);
            return tuple2s;
        }
         return tuple2s;
    }

}
