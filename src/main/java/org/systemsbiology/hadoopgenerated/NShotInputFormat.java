package org.systemsbiology.hadoopgenerated;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import java.io.*;

/**
 * org.systemsbiology.hadoopgenerated.NSHotInputFormat
 *    code sample showing how to override AbstractNShotInputFormat
 *    to make real keys
 * @author Steve Lewis
 * @date Oct 23, 2010
 */
public class NShotInputFormat extends AbstractNShotInputFormat<LongWritable,Text>
{
    public static NShotInputFormat[] EMPTY_ARRAY = {};
    public static Class THIS_CLASS = NShotInputFormat.class;

    public NShotInputFormat()
    {
    }

    /**
     * Implement to generate a reasonable key
     *
     * @param index current key index
     * @return non-null key
     */
    @Override
    protected LongWritable getKeyFromIndex(long index)
    {
        return new LongWritable(index);
    }

    /**
     * Implement to generate a ressomable key
     *
     * @param index current key index
     * @return non-null key
     */
    @Override
    protected Text getValueFromIndex(long index)
    {
         return new Text("Shot " + index);
    }



//    public static void main(String[] args) throws Exception
//    {
//        Configuration conf = new Configuration();
//        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
////        if (otherArgs.length != 2) {
////            System.err.println("Usage: wordcount <in> <out>");
////            System.exit(2);
////        }
//        Job job = new Job(conf, "Generated data");
//        job.setJarByClass(NShotInputFormat.class);
//
//
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
//
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//
//
//        job.setInputFormatClass(NShotInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
//
//        // added Slewis
//
//        if (otherArgs.length > 1) {
//            org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job,
//                    new Path(otherArgs[0]));
//        }
//
//        // make sure output does not exist
//        int index = 1;
//        String base = "NShot";
//        String s = base;
//        while (new File(s).exists()) {
//            s = base + index++;
//        }
//        Path outputDir = new Path(s);
//
//
//        FileOutputFormat.setOutputPath(job, outputDir);
//
//
//        boolean ans = job.waitForCompletion(true);
//        int ret = ans ? 0 : 1;
//    }
    
}
