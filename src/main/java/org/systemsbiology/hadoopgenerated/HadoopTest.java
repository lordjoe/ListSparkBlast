package org.systemsbiology.hadoopgenerated;


import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;

/**
 * org.systemsbiology.hadoop.NShot
 * This is a complete Map Reduce example which does not require any data on HDFS. It generated a
 * set of keys and splits based on the   properties  NumberKeys and NumberSplits NumberReducers
 * Locally the arguments are
 * -DNumberKeys=100000  - number of keys
 * -DNumberSplits=50    - number splits
 * -DNumberReducers=3   - number reducers
 * SomeEmptyLocalDirectory
 * <p/>
 * Use the following command line on the cluster
 *    /home/www/hadoop/bin/hadoop  jar jobs/HadoopTest.jar  org.systemsbiology.hadoopgenerated.HadoopTest -DNumberKeys=100000000 -DNumberSplits=50 -DNUmberReducers=5 /user/howdah/NShot/output1
 *
 * /home/www/hadoop/bin/hadoop  - path to hadoop
 * jar jobs/HadoopTest.jar     - path to jar
 * org.systemsbiology.hadoopgenerated.HadoopTest  - main class DO NOT CHANGE
 * -DNumberKeys=100000  - number of keys
 * -DNumberSplits=50    - number splits
 * -DNumberReducers=3   - number reducers
 * SomeEmptyHDFSDirectory
 * <p/>
 * User: steven
 * Date: 11/14/12
 */
public class HadoopTest extends Configured implements Tool {

    /**
     * define the number of keys and spilts emitted
     */
    public static final long NUMBER_KEYS = 1000 * 1000 * 30;
    public static final long NUMBER_SPLITS = 10;
    private static long gNumberKeys = 20;
    private static int gNumberSplits = 5;


    /**
     * this many keys will be sent
     *
     * @return as above
     */
    public static long getNumberKeys() {
        return gNumberKeys;
    }

    /**
     * number send keys
     *
     * @param pNumberKeys as above
     */
    public static void setNumberKeys(long pNumberKeys) {
        gNumberKeys = pNumberKeys;
    }


    /**
     * this many splits will be used
     *
     * @return as above
     */
    public static int getNumberSplits() {
        return gNumberSplits;
    }

    /**
     * number send splite
     *
     * @param pNumberSplits as above
     */
    public static void setNumberSplits(int pNumberSplits) {
        gNumberSplits = pNumberSplits;
    }


    /**
     * A Simple map that adds some text to the key and outputs it
     */
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();
            String numberKeys = configuration.get("NumberKeys");
            if (numberKeys != null)
                setNumberKeys(Long.parseLong(numberKeys));
            String numberSplits = configuration.get("NumberSplits");
            if (numberSplits != null)
                setNumberSplits(Integer.parseInt(numberSplits));

            super.setup(context);
        }

        @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {

            // Add interesting code here
            context.write(new Text("foo" + key), new Text("bar" + key));
        }
    }

    /**
     * utility methof to allow kays and values to be written out
     *
     * @param context  context to write
     * @param key      !null key  Text
     * @param value    !null value Text
     * @param keyText  !null key String
     * @param valueStr !null value String
     */
    @SuppressWarnings("unchecked")
    public static void writeKeyValue(Reducer.Context context, Text key, Text value, String keyText, String valueStr) {
        try {
            key.set(keyText);
            if (valueStr == null)
                valueStr = "null";
            value.set(valueStr);
            context.write(key, value);
        }
        catch (IOException e) {
            throw new RuntimeException(e);

        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);

        }

    }

    /**
     * reducer class - does little but will write out System properties as key values for
     * debugging
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text> {


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration configuration = context.getConfiguration();
            String numberKeys = configuration.get("NumberKeys");
            if (numberKeys != null)
                setNumberKeys(Long.parseLong(numberKeys));
            String numberSplits = configuration.get("NumberSplits");
            if (numberSplits != null)
                setNumberSplits(Integer.parseInt(numberSplits));

            Text dkey = new Text();
            Text dValue = new Text();
            // sneaky trick to extract the version
            String version = VersionInfo.getVersion();
            writeKeyValue(context, dkey, dValue, "Hadoop Version", version);
            writeKeyValue(context, dkey, dValue, "NumberKeys", numberKeys);
            writeKeyValue(context, dkey, dValue, "NumberSplits", numberSplits);

            /**
             * Every reducer starts by writing out the system properties
             * to help determine how the app is running
             */
            Properties properties = System.getProperties();
            for (String key : properties.stringPropertyNames()) {
                writeKeyValue(context, dkey, dValue, key, System.getProperty(key));

            }
        }

        /**
         * This method is called once for each key. Most applications will define
         * their reduce class by overriding this method. The default implementation
         * is an identity function.
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values,
                              Context context)
                throws IOException, InterruptedException {
            // Add interesting code here
            for (Text vCheck : values) {
                context.write(key, vCheck);
            }

        }


    }

    /**
     * emoty out a directory
     *
     * @param fs  !null file system
     * @param src !null path
     */
    @SuppressWarnings("unused")
    public static void expungeDirectory(final FileSystem fs, final Path src) {
        try {
            if (!fs.exists(src))
                return;
            // break these out
            if (fs.getFileStatus(src).isDir()) {
                boolean doneOK = fs.delete(src, true);
                if (!doneOK)
                    throw new IllegalStateException("could not delete file");
                return;
            }
            if (fs.isFile(src)) {
                boolean doneOK = fs.delete(src, false);
                if (!doneOK)
                    throw new IllegalStateException("could not delete file");
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);

        }

    }


    public static class NShotInputFormat extends InputFormat {

        public NShotInputFormat() {
        }

        /**
         * Implement to generate a reasonable key
         *
         * @param index current key index
         * @return non-null key
         */
        protected LongWritable getKeyFromIndex(long index) {
            return new LongWritable(index);
        }

        /**
         * Implement to generate a ressomable key
         *
         * @param index current key index
         * @return non-null key
         */
        protected Text getValueFromIndex(long index) {
            return new Text("Shot " + index);
        }

        /**
         * Create a record reader for a given split. The framework will call
         * {@link org.apache.hadoop.mapreduce.RecordReader#initialize(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)} before
         * the split is used.
         *
         * @param split   the split to be read
         * @param context the information about the task
         * @return a new record reader
         * @throws java.io.IOException
         * @throws InterruptedException
         */
        @Override
        public RecordReader createRecordReader(
                InputSplit split,
                TaskAttemptContext context)
                throws IOException, InterruptedException {
            return new NShotDataRecordReader((NShotInputSplit) split, this);
        }


        /**
         * Logically split the set of input files for the job.
         * <p/>
         * <p>Each {@link org.apache.hadoop.mapreduce.InputSplit} is then assigned to an individual {@link org.apache.hadoop.mapreduce.Mapper}
         * for processing.</p>
         * <p/>
         * <p><i>Note</i>: The split is a <i>logical</i> split of the inputs and the
         * input files are not physically split into chunks. For e.g. a split could
         * be <i>&lt;input-file-path, start, offset&gt;</i> tuple. The InputFormat
         * also creates the {@link org.apache.hadoop.mapreduce.RecordReader} to read the {@link org.apache.hadoop.mapreduce.InputSplit}.
         *
         * @param context job configuration.
         * @return an array of {@link org.apache.hadoop.mapreduce.InputSplit}s for the job.
         */
        @Override
        public List<InputSplit> getSplits(JobContext context)
                throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();
            String numberKeys = configuration.get("NumberKeys");
            if (numberKeys != null)
                setNumberKeys(Long.parseLong(numberKeys));
            String numberSplits = configuration.get("NumberSplits");
            if (numberSplits != null)
                setNumberSplits(Integer.parseInt(numberSplits));


            int numSplits = getNumberSplits();

            ArrayList<InputSplit> splits = new ArrayList<InputSplit>(numSplits);
            for (int i = 0; i < numSplits; i++) {
                final NShotInputSplit inputSplit = new NShotInputSplit();
                inputSplit.setIndex(i);
                splits.add(inputSplit);
            }


            return splits;
        }

    } // end class NShot Inpur format

    /**
     * All the work is done here to generate the keys
     * In this sample we use simple code to generate keys but a
     * different version might use a more sophistocated scheme
     */
    public static class NShotDataRecordReader extends RecordReader<LongWritable, Text> {

        private long m_NFired;
        private final NShotInputSplit m_Splitter;
        private final NShotInputFormat m_Parent;

        public NShotDataRecordReader(NShotInputSplit split, NShotInputFormat parent) {
            m_Parent = parent;
            m_Splitter = split;
            m_NFired = m_Splitter.getIndex(); // start each recorder at different value
        }

        /**
         * Called once at initialization.
         *
         * @param split   the split that defines the range of records to read
         * @param context the information about the task
         * @throws java.io.IOException
         * @throws InterruptedException
         */
        @Override
        public void initialize(InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {

        }

        @SuppressWarnings("unused")
        public NShotInputSplit getSplitter() {
            return m_Splitter;
        }

        public long getNFired() {
            return m_NFired;
        }


        public void close() {
        }

        public float getProgress() {
            return getNFired() / (float) getNumberKeys();
        }

        /**
         * Read the next key, value pair.
         *
         * @return true if a key/value pair was read
         * @throws java.io.IOException
         * @throws InterruptedException
         */
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (getNFired() >= getNumberKeys())
                return false;

            m_NFired += getNumberSplits();
            return true;
        }

        /**
         * Get the current key
         *
         * @return the current key or null if there is no current key
         * @throws java.io.IOException
         * @throws InterruptedException
         */
        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return m_Parent.getKeyFromIndex(m_NFired - getNumberSplits());
        }

        /**
         * Get the current value.  Override for
         *
         * @return the object that was read
         * @throws java.io.IOException
         * @throws InterruptedException
         */
        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return m_Parent.getValueFromIndex(m_NFired - getNumberSplits());
        }
    }    // end class NShotDataRecordReader

    @SuppressWarnings(value = "deprecated")
    public static class NShotInputSplit extends InputSplit
            // NOTE IF THE DEPRECATED INTERFACE IS NOT IMP{LEMENTED THE OBJECT WILL NOT SERIALIZE
            implements org.apache.hadoop.mapred.InputSplit {

        private long m_Index;

        public NShotInputSplit() {
        }


        public long getIndex() {
            return m_Index;
        }

        public void setIndex(long pIndex) {
            m_Index = pIndex;
        }

        public long getLength() {
            return getNumberKeys() / getNumberSplits();
        }

        public String[] getLocations() throws IOException {
            return new String[]{};
        }

        public void readFields(DataInput in) throws IOException {
            setIndex(in.readLong());
        }

        public void write(DataOutput out) throws IOException {
            out.writeLong(this.m_Index);
        }


    } // end class NShotDataRecordReader


    /**
     * Execute the command with the given arguments.
     *
     * @param args command specific arguments.
     *             o
     * @return exit code.
     * @throws Exception
     */
    @Override
    public int run(final String[] otherArgs) throws Exception {
        Configuration conf =  getConf();
        //      conf.set(BamHadoopUtilities.CONF_KEY,"config/MotifLocator.config");

        String numberKeys = conf.get("NumberKeys");
        if (numberKeys != null)
            setNumberKeys(Long.parseLong(numberKeys));
        String numberSplits = conf.get("NumberSplits");
        if (numberSplits != null)
            setNumberSplits(Integer.parseInt(numberSplits));

        int nReducers = 1;
        String numberReducers = conf.get("NumberReducers");
        if (numberReducers != null)
            nReducers = Integer.parseInt(numberReducers);

        //        if (otherArgs.length != 2) {
        //            System.err.println("Usage: wordcount <in> <out>");
        //            System.exit(2);
        //        }
        Job job = new Job(conf, "Generated data");
        job.setJarByClass(HadoopTest.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        job.setInputFormatClass(NShotInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(nReducers);

        // added Slewis


        String athString = otherArgs[otherArgs.length - 1];

        Path outputDir = new Path(athString);



          FileOutputFormat.setOutputPath(job, outputDir);


        boolean ans = job.waitForCompletion(true);
        int ret = ans ? 0 : 1;
        return ret;
    }

    /**
      * kill a directory and all contents
      * @param src
      * @param fs
      * @return
      */
         public static  boolean expunge(Path src,FileSystem fs) {


           try {
               if (!fs.exists(src))
                   return true;
               // break these out
               if (fs.getFileStatus(src).isDir()) {
                   boolean doneOK = fs.delete(src, true);
                   doneOK = !fs.exists(src);
                   return doneOK;
               }
               if (fs.isFile(src)) {
                   boolean doneOK = fs.delete(src, false);
                   return doneOK;
               }
               throw new IllegalStateException("should be file of directory if it exists");
           }
           catch (IOException e) {
               throw new RuntimeException(e);
           }

       }


    /**
     * @param args These should be
     *             -DNumberKeys=100000 -DNumberSplits=50 -DNumberReducers=3 <output_dir>
     *             where  <output_dir> does not exist
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        HadoopTest nShot = new HadoopTest();
        nShot.run(args);
    }


}
