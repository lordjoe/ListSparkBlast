package com.lordjoe.sparkblast;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.HashMap;
import java.util.Map;

/**
 * com.lordjoe.sparkblast.HeaderOrder
 * User: Steve
 * Date: 10/4/21
 */
public class HeaderOrder {
    public static final HeaderOrder[] EMPTY_ARRAY = {};

    /**
     * map header lines to number so we can reorder
     * @param fasta
     * @return
     */
    public static Map<String,Integer> readHeaderOrder1(File fasta) {
        try {
            Map<String,Integer> ret =  new HashMap<>();
            int itemNumber = 0;
            LineNumberReader rdr = new LineNumberReader(new FileReader(fasta)) ;
            String line = rdr.readLine();
            while(line != null)  {
                if(line.startsWith("Query=")) {
                    String s = line.substring("Query=".length());
                    if(s.endsWith(",")) {
                        line = rdr.readLine();
                        s += line;
                    }
                    ret.put(s,itemNumber++);
                }
                line = rdr.readLine();
            }
            return ret;
        } catch (IOException e) {
            throw new RuntimeException(e);

        }
    }

    public static String stripNonLettersAndNumbers(String s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if(Character.isLetter(c) || Character.isDigit(c))
                sb.append(Character.toUpperCase(c));
         }
          return sb.toString();
    }

    public static Map<String,Integer> readHeaderOrder(File fasta) {
        try {
            Map<String,Integer> ret =  new HashMap<>();
            int itemNumber = 0;
            LineNumberReader rdr = new LineNumberReader(new FileReader(fasta)) ;
            String line = rdr.readLine();
            while(line != null)  {
                if(line.startsWith(">")) {
                    String s = line.substring(">".length());
                    ret.put(stripNonLettersAndNumbers(s),itemNumber++);
                }
                line = rdr.readLine();
            }
            return ret;
        } catch (IOException e) {
            throw new RuntimeException(e);

        }
    }

    /**
     * test call
     * @param args
     */
    public static void main(String[] args) {
        Map<String,Integer> list = readHeaderOrder(new File(args[0]));
        int size = list.size();
    }
}
