package com.refactorlabs.cs378.assign5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Arrays;

/**
 * Example MapReduce program that performs word count.
 * Modified for Assignment 5
 *
 * @author David Franke (dfranke@cs.utexas.edu)
 */
public class WordCountFive {

    /**
     * Each count output from the map() function is "1", so to minimize small
     * object creation we can use a constant for this output value/object.
     */
    public final static LongWritable ONE = new LongWritable(1L);

    // data set headers from dataSet5.tsv
    public final static String[] dataSetHeaders = new String[]{"user_id", "event_type",	"page", "referring_domain",
            "event_timestamp", "city", "vin",	"vehicle_condition", "year", "make",
            "model", "trim",	"body_style",  "cab_style", "price", "mileage",             
            "image_count", "carfax_free_report", "features"};

    // these headers have a large number of unique values... ignore
    public final static String[] ignoreHeaders = new String[]{"event_timestamp", "image_count", "initial_price",
            "mileage", "referrering_domain", "user_id", "vin"};

    /**
     * The Map class for word count.  Extends class Mapper, provided by Hadoop.
     * This class defines the map() function for the word count example.
     */
    public static class MapClass extends Mapper<LongWritable, Text, Text, LongWritable> {

        /**
         * Counter group for the mapper.  Individual counters are grouped for the mapper.
         */
        private static final String MAPPER_COUNTER_GROUP = "Mapper Counts";

        /**
         * Local variable "word" will contain the word identified in the input.
         * The Hadoop Text object is mutable, so we can reuse the same object and
         * simply reset its value as each word in the input is encountered.
         */
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            // get all tokens by splitting them by the tab characters
            String[] tokens = line.split("\\t");

            // run through all tokens
            for (int index = 0; index < tokens.length; index++) {
                // current token
                String currentToken = tokens[index];
                // current header
                String currentHeader = dataSetHeaders[index];

                // check to see if currentHeader is in the ignoreHeaders Array
                // if not, go ahead and count the word
                if (!Arrays.asList(ignoreHeaders).contains(currentHeader)) {
                    // fieldname:value
                    word.set(currentHeader + ":" + currentToken);

                    context.write(word, ONE);
                    context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
                }
            }
        }
    }

    /**
     * Reducer class from Assignment 1's WordCount
     * The Reduce class for word count.  Extends class Reducer, provided by Hadoop.
     * This class defines the reduce() function for the word count example.
     */
    public static class ReduceClass extends Reducer<Text, LongWritable, Text, LongWritable> {

        /**
         * Counter group for the reducer.  Individual counters are grouped for the reducer.
         */
        private static final String REDUCER_COUNTER_GROUP = "Reducer Counts";

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0L;

            context.getCounter(REDUCER_COUNTER_GROUP, "Words Out").increment(1L);

            // Sum up the counts for the current word, specified in object "key".
            for (LongWritable value : values) {
                sum += value.get();
            }
            // Emit the total count for the word.
            context.write(key, new LongWritable(sum));
        }
    }

    /**
     * The main method specifies the characteristics of the map-reduce job
     * by setting values on the Job object, and then initiates the map-reduce
     * job and waits for it to complete.
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        @SuppressWarnings("deprecation")
		Job job = new Job(conf, "WordCount");
        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(WordCountFive.class);

        // Set the output key and value types (for map and reduce).
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // Set the map and reduce classes.
        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);

        // Set the input and output file formats.
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Grab the input file and output directory from the command line.
        FileInputFormat.addInputPath(job, new Path(appArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

        // Initiate the map-reduce job, and wait for completion.
        job.waitForCompletion(true);
    }
}