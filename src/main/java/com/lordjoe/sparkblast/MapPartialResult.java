package com.lordjoe.sparkblast;

import com.lordjoe.sparkutilities.Utilities;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.File;
import java.io.PrintWriter;
import java.util.*;

/**
 * com.lordjoe.sparkblast.MapPartialResult
 * User: Steve
 * Date: 10/5/21
 */
public class MapPartialResult implements PairFlatMapFunction<String, Integer, String> {
    public final Map<String, Integer> ordering;
    private StringBuilder sb = new StringBuilder();
    private StringBuilder ob = new StringBuilder();
    private Integer order = null;
    private boolean inquery = false;
    private boolean querySeen = false;
    private boolean inorder = false;
    private boolean isZeroLength   = true;

    private String orderTest;

    MapPartialResult(Map<String, Integer> m_ordering) {
        ordering = m_ordering;
    }

    public static Integer findOrder(StringBuilder ob,Map<String, Integer> ordering)    {
        String qt = ob.toString();
        String o = HeaderOrder.stripNonLettersAndNumbers(qt);
        if(o.startsWith("TR"))
            o = o.substring(2);
        if(o.startsWith("SP"))
            o = o.substring(2);

        Integer order = ordering.get(o) ;
        if(order == null) {
            String orderTest = qt;
        }
        if(order != null)
            ob.setLength(0);
        return  order;
    }

    @Override
    public Iterable<Tuple2<Integer, String>> call(String line) throws Exception {
        List<Tuple2<Integer, String>> tuple2s = new ArrayList<Tuple2<Integer, String>>();
        int i = 0;

        if(line.startsWith("Sbjct  23   DAPGSVNKLDTKTVANLG----EALNVLEKQSELKGLLLRSAK-TALIVGAD----ITEF  73"))
            i = 0;
 
        if(line.contains("Length="))
           isZeroLength = line.equals("Length=0")  ;
          if(querySeen && line.contains("Database:")) {
               String s = sb.toString();
               if(order == null)
                   order = findOrder(ob,ordering) ;
            if(order != null && !isZeroLength) {
                String qt = ob.toString();

                if(order != null && order > 3590)
                    qt = ob.toString();
                tuple2s.add(new Tuple2<>(order, s));
                sb.setLength(0);
                order = null;
                inquery = false;
            }
            else {
                inquery = false;
                i = 0;
            }
        }
        if (!line.startsWith("Query=") )  {
            if(line.contains("gi|490646412|ref|WP_004511407.1"))
                   i = 0;

            if(inquery)   {
               sb.append(line + "\n");
           }
           if(inorder)  {
               String added = line.trim();
               if(added.equals("")) {
                  order =  findOrder(ob,ordering) ;
                  if(order != null && order > 3590)
                      inorder = false;

                   inorder = false;
               }
               else {
                   ob.append(line);
               }
           }
        }
        else {    // Line starts Query=
            querySeen = true;
            String s = sb.toString();

            if(s.contains("490646412"))
                i = 0;
            if(s.contains("490646408"))
                i = 0;
            if(order != null && !isZeroLength) {
                tuple2s.add(new Tuple2<>(order, s));
              }
            sb.setLength(0);
            String trim = line.trim();
            trim = trim.replace("tr|>sp|>gi|","gi"); // not sure why these are added
            sb.append(trim);
            sb.append("\n");
            isZeroLength = false;
            String quuryText = line.substring("Query=".length());
            ob.append(quuryText);
            inorder = true;
            inquery = true;
            order = ordering.get(HeaderOrder.stripNonLettersAndNumbers(quuryText)) ;
            if(order != null && order > 3590)
                inorder = true;
            if(order == null)
                order = ordering.get(HeaderOrder.stripNonLettersAndNumbers(quuryText)) ;
        }
        return tuple2s;
    }

    public static Map<Integer, String> invert(Iterable<Tuple2<Integer, String>> l) {
        Map<Integer, String> ret = new HashMap<>();
        Iterator<Tuple2<Integer, String>> iterator = l.iterator();
        while (iterator.hasNext()) {
            Tuple2<Integer, String> next = iterator.next();
            Integer integer = next._1;
            String s = next._2;
            ret.put(integer, s);
        }
        return ret;
    }

    public static List<String> fromTuples(Iterable<Tuple2<Integer, String>> l) {
        List<String> s = new ArrayList<>();
        Map<Integer, String> inv = invert(l);
        List<Integer> keys = new ArrayList(inv.keySet());
        Collections.sort(keys);
        for (Integer key : keys) {
            String e = inv.get(key);
            if(e.contains(" Database"))
                e = e.substring(0,e.indexOf(" Database") - 1) ;
            s.add(e);
        }
        return s;
    }

    public static String extractHeaderAndFooter(String file,String[] footerHolder) {
        StringBuilder sb = new StringBuilder();
        StringBuilder sb2 = new StringBuilder();
        String[] lines = file.split("\n");
        int i = 0;
        for ( ; i < lines.length; i++) {
            String line = lines[i];
            if (line.startsWith("Query=")) {
                break;
            }
            sb.append(line);
            sb.append("\n");
        }
        for ( ; i < lines.length; i++) {
            String line = lines[i];
            if (line.startsWith("  Database:")) {
                break;
            }
          }
        for ( ; i < lines.length; i++) {
            String line = lines[i];
            sb2.append(line);
            sb2.append("\n");
        }
        footerHolder[0] = sb2.toString();

        return sb.toString();
     }

    public static void main(String[] args) throws Exception {
        File fasta = new File(args[0]);
        Map<String, Integer> list = HeaderOrder.readHeaderOrder(fasta);
        File dir = new File(args[1]);
        MapPartialResult me = new MapPartialResult(list);

        String[] files = dir.list();

        String header = null;
        String footer = null;
        String[] footerHolder  = new String[1];
        for (String file : files) {
            if(file.startsWith("part"))   {
                File f = new File(dir,file);
                String contents = Utilities.readInFile(f);
                 header = extractHeaderAndFooter(contents,footerHolder) ;
                break;
            }
        }
        footer =  footerHolder[0];
        List<Tuple2<Integer, String>> tuples = new ArrayList<>();
        for (String file : files) {
            if(file.startsWith("part"))   {
                File f = new File(dir,file);
                String contents = Utilities.readInFile(f);
                Iterable<Tuple2<Integer, String>> items = me.call(contents);
                tuples.addAll((ArrayList<Tuple2<Integer, String>>)items);
            }
        }
        Collections.sort(tuples,new Comparator<Tuple2<Integer, String>>( ) {
            public int compare(Tuple2<Integer, String> var1, Tuple2<Integer, String> var2) {
                return var1._1.compareTo(var2._1);
            }
         }) ;
         List<String> queries = fromTuples(tuples);

        File outdir = null;
        String outFileName = args[2];
        File outf = new File(outFileName);
       int index = outFileName.lastIndexOf("/");
        if(index > -1)   {
            outdir = new File(outFileName.substring(0,index));
            outFileName = outFileName.substring(index + 1);
            outf = new File(outdir,outFileName);
        }
        PrintWriter pw = new PrintWriter(outf) ;
        pw.print(header);
        for (String query : queries) {
            if(!query.startsWith("Query="))
                throw new UnsupportedOperationException("Fix This"); // ToDo
            if(query.startsWith("  Database:")) {
               continue;
            }
             pw.print(query);
        }
        pw.print(footer);
        pw.close();

    }
}
