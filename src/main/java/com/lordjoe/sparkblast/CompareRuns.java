package com.lordjoe.sparkblast;

import java.io.File;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * com.lordjoe.sparkblast.CompareRuns
 * User: Steve
 * Date: 10/14/21
 */
public class CompareRuns {
    
    public static void addItem(Queue q, String s)  {
        if(q.size() == 16)
            q.remove();
        q.add(s);
    }

    public static void main(String[] args) throws Exception {
        File oneRun = new File(args[0]);
        File merged = new File(args[1]);
        LineNumberReader rdr1 = new LineNumberReader(new FileReader(oneRun)) ;
        LineNumberReader rdr2 = new LineNumberReader(new FileReader(merged)) ;
        LinkedBlockingDeque<String> strings1 = new LinkedBlockingDeque(16);
        LinkedBlockingDeque<String> strings2 = new LinkedBlockingDeque(16);


        long nlines = 0;
        String line1 = rdr1.readLine();
        String line2 = rdr2.readLine();
        addItem(strings1,line1);
        addItem(strings2,line2);
        while(line1 != null) {
            if(!line1.equals(line2))   {
                while(line1.startsWith("Query=") && line2.equals("")) {
                    line2 = rdr2.readLine();
                    addItem(strings2,line2);
                }
                // query lines might break in strange places
                if(line1.startsWith("Query=") && line2.startsWith("Query="))  {
                    String line1x = rdr1.readLine();
                    addItem(strings1,line1x);
                    while(!line1x.startsWith("Length="))    {
                        line1 += line1x;
                        nlines++;
                        line1x = rdr1.readLine();
                        addItem(strings1,line1x);
                      }
                    String line2x = rdr2.readLine();
                    addItem(strings2,line2x);
                    while(!line2x.startsWith("Length="))    {
                        line2 += line2x;
                        line2x = rdr2.readLine();
                        addItem(strings2,line2x);
                    }
                    if(!line1x.equals(line2x)) {
                        System.out.println("Unequal at " + nlines + "\n" + line1x + "\n" + line2x + "\n");
                        throw new UnsupportedOperationException("Fix This"); // ToDo
                    }

                }
                if(!line1.equals(line2)) {
                    line1 = line1.replace(" ","");
                    line2 = line2.replace(" ","");
                    if(!line1.equals(line2)) {
                        System.out.println("Unequal at " + nlines + "\n" + line1 + "\n" + line2 + "\n");
                        throw new UnsupportedOperationException("Fix This"); // ToDo
                    }

                }
            }
            line1 = rdr1.readLine();
            line2 = rdr2.readLine();
            if(line1 != null )
              addItem(strings1,line1);
            if(line2 != null )
                addItem(strings2,line2);
            nlines++;
        }
        System.out.println("Files are identical for " + nlines);
    }

}
