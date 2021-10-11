package com.lordjoe.sparkblast;

import java.io.*;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * com.lordjoe.sparkblast.ShortenFasta
 * User: Steve
 * Date: 6/29/21
 */
public class ShortenFasta {
    public static final Random RND = new Random();

    public static void main(String[] args) throws IOException {
        File input = new File(args[0]);
        Set<String> proteins  = findProteins(input);
        File fullData = new File(args[1]);
        LineNumberReader rdr = new LineNumberReader(new FileReader(fullData)) ;
        File output = new File(args[2]);
        PrintWriter out = new PrintWriter(new FileWriter(output));
        double fraction = 1;
        if(args.length > 3)
            fraction = Double.parseDouble(args[3]);
        filterProteins(proteins,rdr,out, fraction);
    }

    private static void filterProteins(Set<String> proteins, LineNumberReader rdr, PrintWriter out, double fraction) {
        try {
            String line = rdr.readLine();
            while(line != null)  {
                if(line.startsWith(">"))   {
                    String prot = line.substring(1,line.indexOf(" "));
                    boolean keep = fraction == 1 || RND.nextDouble() <= fraction;
                    if(keep && proteins.contains(prot)) {
                        out.println(line);
                        line = rdr.readLine();
                        while(line != null)  {
                           if(line.startsWith(">"))
                               break;
                            out.println(line);
                            line = rdr.readLine();
                        }
                      }
                    else {
                        line = rdr.readLine();
                    }
                }
                else {
                    line = rdr.readLine();
                }
            }
            out.close();
        } catch (IOException e) {
            throw new RuntimeException(e);

        }

    }

    private static Set<String> findProteins(File input) {
        try {
            Set<String> ret = new HashSet<>();
            LineNumberReader rdr = new LineNumberReader(new FileReader(input)) ;
            String line = rdr.readLine();
            while(line != null)  {
                if(line.startsWith(">"))   {
                   String prot = line.substring(1,line.indexOf(" "));
                   ret.add(prot);
                }
                line = rdr.readLine();
            }
            return ret;
        } catch (IOException e) {
            throw new RuntimeException(e);

        }
    }
}
