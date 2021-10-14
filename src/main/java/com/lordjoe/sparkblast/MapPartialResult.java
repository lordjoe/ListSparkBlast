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

    MapPartialResult(Map<String, Integer> m_ordering) {
        ordering = m_ordering;
    }

    @Override
    public Iterable<Tuple2<Integer, String>> call(String s) throws Exception {
        List<Tuple2<Integer, String>> ret = new ArrayList<Tuple2<Integer, String>>();
         StringBuilder sb = new StringBuilder();
        Integer order = null;
        String[] lines = s.split("\n");
        int i = 0;
        String l = lines[i++];
        while (!l.startsWith("Query="))  {
            l = lines[i++];
        }
        i--;
        for (; i < lines.length; i++) {
            String line = lines[i];
            if (line.startsWith("Query=")) {
                if(order != null) {
                    ret.add(new Tuple2<>(order, sb.toString())) ;
                }
                else {
                    if(sb.length() > 0) {
                         String sx = sb.toString();
                         if(sx.startsWith("Query")  && !sx.contains("Length=0\n"))
                             throw new UnsupportedOperationException("Fix This"); // ToDo
                    }
                }
                sb = new StringBuilder();

                String trim = line.trim();
                trim = trim.replace("tr|>sp|>gi|","gi"); // not sure why these are added
                sb.append(trim);
                sb.append("\n");
                String quuryText = line.substring("Query=".length());
                line = lines[++i];
                while(!line.startsWith("Length="))  {
                     sb.append(line.trim());
                    sb.append("\n");
                    quuryText += line;
                    line = lines[++i];
                }
                sb.append(line.trim());
                sb.append("\n");
                boolean isZeroLength = line.equals("Length=0")  ;

                order = ordering.get(HeaderOrder.stripNonLettersAndNumbers(quuryText)) ;
                int next = quuryText.indexOf(">") ;
                while(order == null && next> -1)  {
                    quuryText = quuryText.substring(next + 1);
                     next = quuryText.indexOf(">") ;
                    String o = HeaderOrder.stripNonLettersAndNumbers(quuryText);
                    order = ordering.get(o) ;
                    if(order != null )
                        break;
                }
                if(order == null && next == -1 && !isZeroLength)
                    throw new UnsupportedOperationException("Fix This"); // ToDo
              }
            else {
                sb.append(line);
                sb.append("\n");
            }

        }
        if(order != null) {
            ret.add(new Tuple2<>(order, sb.toString())) ;
        }
        return ret;
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
