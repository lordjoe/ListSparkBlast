package com.lordjoe.sparkblast;


import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * com.lordjoe.sparkblast.BlastWriter
 * User: Steve
 * Date: 10/18/21
 */
public class BlastWriter implements Function<Tuple2<Integer,String>,Object> {

    public   final String filePath;
    public   final String header;
    private File outFile;
    private int NQueries;
    private PrintWriter out;
    private int queriesSeen;

    public BlastWriter(String outFile, String header, int NQueries) {
        this.filePath = outFile;
        this.header = header;
        this.NQueries = NQueries;
     }



    @Override
    public Object call(Tuple2<Integer, String> t2) throws Exception {
        guaranteeFile();
        int n = t2._1;
        if(n > 3588)
            n = t2._1;
        out.println(t2._2);
            queriesSeen++;
        if(NQueries <+ queriesSeen)
              out.close();
        out.flush();
        return null;
    }

    private void guaranteeFile() {
        try {
            if(outFile == null) {
                outFile = new File(filePath);
                out = new PrintWriter(new FileWriter(outFile))   ;
                out.print(header);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);

        }
    }
}
