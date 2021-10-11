package org.systemsbiology.hadoopgenerated;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
 import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;

///**
// * org.systemsbiology.hadoopgenerated.OneShotHadoop
// *
// * @author Steve Lewis
// * @date Oct 10, 2010
// */
//public class OneShotHadoop
//{
//    public static OneShotHadoop[] EMPTY_ARRAY = {};
//    public static Class THIS_CLASS = OneShotHadoop.class;
//
//
//    public static class Map extends Mapper<LongWritable, Text, Text, Text>
//    {
//
//        @Override
//        protected void map(LongWritable key, Text value,
//                           Context context) throws IOException, InterruptedException
//        {
//
//            // Add interesting code here
//            context.write(new Text("foo"), new Text("bar"));
//        }
//    }
//
//    public static class Reduce extends Reducer<Text, Text, Text, Text>
//    {
//
//        /**
//         * This method is called once for each key. Most applications will define
//         * their reduce class by overriding this method. The default implementation
//         * is an identity function.
//         */
//        @Override
//        protected void reduce(Text key, Iterable<Text> values,
//                              Context context)
//                throws IOException, InterruptedException
//        {
//            Iterator<Text> itr = values.iterator();
//            // Add interesting code here
//            while (itr.hasNext()) {
//                Text vCheck = itr.next();
//                context.write(key, vCheck);
//            }
//
//        }
//
//
//    }
//
//    public static class OneShotInputFormat extends InputFormat<LongWritable, Text>
//    {
//
//        /**
//         * Logically split the set of input files for the job.
//         * <p/>
//         * <p>Each {@link org.apache.hadoop.mapreduce.InputSplit} is then assigned to an individual {@link org.apache.hadoop.mapreduce.Mapper}
//         * for processing.</p>
//         * <p/>
//         * <p><i>Note</i>: The split is a <i>logical</i> split of the inputs and the
//         * input files are not physically split into chunks. For e.g. a split could
//         * be <i>&lt;input-file-path, start, offset&gt;</i> tuple. The InputFormat
//         * also creates the {@link org.apache.hadoop.mapreduce.RecordReader} to read the {@link org.apache.hadoop.mapreduce.InputSplit}.
//         *
//         * @param context job configuration.
//         * @return an array of {@link org.apache.hadoop.mapreduce.InputSplit}s for the job.
//         */
//        @Override
//        public List<org.apache.hadoop.mapreduce.InputSplit> getSplits(JobContext context)
//                throws IOException, InterruptedException
//        {
//            int numSplits = 1;
//
//
//            ArrayList<InputSplit> splits = new ArrayList<InputSplit>(numSplits);
//
//            splits.add(new OneShotInputSplit());
//
//
//            return splits;
//        }
//
//
//        /**
//         * Create a record reader for a given split. The framework will call
//         * {@link org.apache.hadoop.mapreduce.RecordReader#initialize(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)} before
//         * the split is used.
//         *
//         * @param split   the split to be read
//         * @param context the information about the task
//         * @return a new record reader
//         * @throws java.io.IOException
//         * @throws InterruptedException
//         */
//        @Override
//        public RecordReader<LongWritable, Text> createRecordReader(
//                org.apache.hadoop.mapreduce.InputSplit split,
//                TaskAttemptContext context)
//                throws IOException, InterruptedException
//        {
//            return new OneShotDataRecordReader((OneShotInputSplit) split);
//        }
//
//
//
//    }
//
//    public static class OneShotDataRecordReader extends RecordReader<LongWritable, Text>
//    {
//
//        private boolean m_Fired;
//        public OneShotDataRecordReader(OneShotInputSplit split)
//        {
//        }
//
//        /**
//         * Called once at initialization.
//         *
//         * @param split   the split that defines the range of records to read
//         * @param context the information about the task
//         * @throws java.io.IOException
//         * @throws InterruptedException
//         */
//        @Override
//        public void initialize(InputSplit split, TaskAttemptContext context)
//                throws IOException, InterruptedException
//        {
//
//        }
//
//        public boolean isFired()
//        {
//            return m_Fired;
//        }
//
//        public void setFired(boolean pFired)
//        {
//            m_Fired = pFired;
//        }
//
//        public void close()
//        {
//        }
//
//        public float getProgress()
//        {
//            return 0.0f;
//        }
//
//        public long getPos()
//        {
//            return 1;
//        }
//
//        public boolean next(LongWritable key, Text value)
//        {
//             if(isFired())
//                 return false;
//
//            setFired(true);
//            return true;
//        }
//
//
//        /**
//         * Read the next key, value pair.
//         *
//         * @return true if a key/value pair was read
//         * @throws java.io.IOException
//         * @throws InterruptedException
//         */
//        @Override
//        public boolean nextKeyValue() throws IOException, InterruptedException
//        {
//            if(isFired())
//                 return false;
//
//            setFired(true);
//            return true;
//         }
//
//        /**
//         * Get the current key
//         *
//         * @return the current key or null if there is no current key
//         * @throws java.io.IOException
//         * @throws InterruptedException
//         */
//        @Override
//        public LongWritable getCurrentKey() throws IOException, InterruptedException
//        {
//            return new LongWritable(1);
//        }
//
//        /**
//         * Get the current value.
//         *
//         * @return the object that was read
//         * @throws java.io.IOException
//         * @throws InterruptedException
//         */
//        @Override
//        public Text getCurrentValue() throws IOException, InterruptedException
//        {
//            return new Text("foo");
//        }
//    }
//
//    @SuppressWarnings(value = "deprecated")
//    public static class OneShotInputSplit extends InputSplit implements
//            org.apache.hadoop.mapred.InputSplit
//    {
//
//        public OneShotInputSplit()
//        {
//        }
//
//
//        public long getLength()
//        {
//            return 1;
//        }
//
//        public String[] getLocations() throws IOException
//        {
//            return new String[]{};
//        }
//
//        public void readFields(DataInput in) throws IOException
//        {
//        }
//
//        public void write(DataOutput out) throws IOException
//        {
//        }
//
//
//    }
//
//
//    public static void main(String[] args) throws Exception
//    {
//        Configuration conf = new Configuration();
//        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
////        if (otherArgs.length != 2) {
////            System.err.println("Usage: wordcount <in> <out>");
////            System.exit(2);
////        }
//        Job job = new Job(conf, "Generated data");
//        job.setJarByClass(OneShotHadoop.class);
//
//        job.setMapperClass(Map.class);
//        job.setReducerClass(Reduce.class);
//
//
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
//
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//
//
//        job.setInputFormatClass(OneShotInputFormat.class);
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
//        String base = "Oneshot"  + (System.currentTimeMillis() / 1000) % 1000;
//        Path outputDir = new Path(base);
//
//
//        FileOutputFormat.setOutputPath(job, outputDir);
//
//
//        boolean ans = job.waitForCompletion(true);
//        int ret = ans ? 0 : 1;
//    }
//}
