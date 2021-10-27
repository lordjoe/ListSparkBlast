package com.lordjoe.sparkblast;

import org.apache.spark.api.java.function.Function;

import java.io.File;
import java.io.PrintWriter;

/**
 * com.lordjoe.sparkblast.HeaderFooterExtractor
 * User: Steve
 * Date: 10/19/21
 */
public class HeaderFooterExtractor  implements Function< String ,Object>  {
    public   final StringBuilder header = new StringBuilder() ;
    public   final StringBuilder footer = new StringBuilder() ;
    public   final String tempName;
    private File outFile;
    private PrintWriter out;
    private boolean buildingHeader = true;
    private boolean buildingFooter = false;
    private boolean footerBuilt = false;

    public HeaderFooterExtractor(String tempName) {
        this.tempName = tempName;
       }



    @Override
    public Object call(  String t2) throws Exception {
          if(footerBuilt)
              return null;
           if(buildingHeader)  {
               if(tempName.startsWith("Query=")) {
                   writeHeader();
               }
               else {
                   header.append(t2);
                   header.append("\n");
               }
            }

           return null;
    }

    private void writeHeader() {
        buildingHeader = false;

    }


}
