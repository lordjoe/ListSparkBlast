package com.lordjoe.sparkblast;

import com.lordjoe.sparkutilities.Utilities;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.File;
import java.io.PrintWriter;
import java.util.*;

/**
 * com.lordjoe.sparkblast.MapQueries
 * User: Steve
 * Date: 10/26/21
 */
public class MapQueries implements PairFlatMapFunction<String, Integer, String> {
    public final Map<String, Integer> ordering;
    private StringBuilder sb = new StringBuilder();
    private StringBuilder ob = new StringBuilder();
    private Integer order = null;
    private boolean inquery = false;
    private boolean querySeen = false;
    private boolean inorder = false;
    private boolean inLast = false;

    private String orderTest;

    MapQueries(Map<String, Integer> m_ordering) {
        ordering = m_ordering;
    }


    @Override
    public Iterable<Tuple2<Integer, String>> call(String line) throws Exception {

        List<Tuple2<Integer, String>> tuple2s = new ArrayList<Tuple2<Integer, String>>();
        if(inLast)
            System.out.println(line);

        if (line.startsWith("Effective search space used:")) {
            sb.append(line);
            sb.append("\n\n");
            if (order != null) {
                String s = sb.toString();
                tuple2s.add(new Tuple2<>(order, s));
            }
            else {
                System.out.println("No Order "+ sb.toString());
            }
            inquery = false;
            inorder = false;
            return tuple2s;
        }

        if (line.startsWith("Query=")) {
            if(inLast)
                throw new UnsupportedOperationException("Fix This"); // ToDo
            if(line.contains("490646408"))
                inLast = true;

            querySeen = true;
            sb.setLength(0);
            String trim = line.trim();
            trim = trim.replace("tr|>sp|>gi|", "gi"); // not sure why these are added
            sb.append(trim);
            sb.append("\n");
             String quuryText = line.substring("Query=".length());
            ob.setLength(0);
            ob.append(quuryText);
            inorder = true;
            inquery = true;

        }
        else {
            if (inquery) {
                sb.append(line + "\n");
            }
            if (inorder) {
                String added = line.trim();
                if (added.equals("")) {
                    order = MapPartialResult.findOrder(ob, ordering);
                    inorder = false;
                } else {
                    ob.append(line);
                }
            }
        }
         return tuple2s;
    }


    public static void main(String[] args) throws Exception {
        File fasta = new File(args[0]);
        Map<String, Integer> list = HeaderOrder.readHeaderOrder(fasta);
        File dir = new File(args[1]);
        MapPartialResult me = new MapPartialResult(list);

        String[] files = dir.list();

        String header = null;
        String footer = null;
        String[] footerHolder = new String[1];
        for (String file : files) {
            if (file.startsWith("part")) {
                File f = new File(dir, file);
                String contents = Utilities.readInFile(f);
                header = MapPartialResult.extractHeaderAndFooter(contents, footerHolder);
                break;
            }
        }
        footer = footerHolder[0];
        List<Tuple2<Integer, String>> tuples = new ArrayList<>();
        for (String file : files) {
            if (file.startsWith("part")) {
                File f = new File(dir, file);
                String contents = Utilities.readInFile(f);
                Iterable<Tuple2<Integer, String>> items = me.call(contents);
                tuples.addAll((ArrayList<Tuple2<Integer, String>>) items);
            }
        }
        Collections.sort(tuples, new Comparator<Tuple2<Integer, String>>() {
            public int compare(Tuple2<Integer, String> var1, Tuple2<Integer, String> var2) {
                return var1._1.compareTo(var2._1);
            }
        });
        List<String> queries = MapPartialResult.fromTuples(tuples);

        File outdir = null;
        String outFileName = args[2];
        File outf = new File(outFileName);
        int index = outFileName.lastIndexOf("/");
        if (index > -1) {
            outdir = new File(outFileName.substring(0, index));
            outFileName = outFileName.substring(index + 1);
            outf = new File(outdir, outFileName);
        }
        PrintWriter pw = new PrintWriter(outf);
        pw.print(header);
        for (String query : queries) {
            if (!query.startsWith("Query="))
                throw new UnsupportedOperationException("Fix This"); // ToDo
            if (query.startsWith("  Database:")) {
                continue;
            }
            pw.print(query);
        }
        pw.print(footer);
        pw.close();

    }
}

