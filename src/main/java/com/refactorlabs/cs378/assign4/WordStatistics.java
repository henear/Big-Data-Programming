package com.refactorlabs.cs378.assign4;

//import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * WordCount example using AVRO defined class for the word count data,
 * to demonstrate how to use AVRO defined objects.
 */
public class WordStatistics extends Configured implements Tool {

    /**
     * The Reduce class for word count.  Extends class Reducer, provided by Hadoop.
     * This class defines the reduce() function for the word count example.
     */
    public static class ReduceClass extends Reducer<Text, AvroValue<WordStatisticsData>,
            Text, AvroValue<WordStatisticsData>> {

        @Override
        public void reduce(Text key, Iterable<AvroValue<WordStatisticsData>> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0L;
            long ttcount = 0L;
            long ssq = 0L;
            long min = Long.MAX_VALUE;
            long max = 0L;
            double mean = 0.0;
            double var = 0.0;
            for (AvroValue<WordStatisticsData> value : values) {
                sum += value.datum().getCount();
                ttcount += value.datum().getTtcount();
                ssq += value.datum().getSsq();
                if(min > value.datum().getMin()){
                	min = value.datum().getMin();
                }
                if(max < value.datum().getMax()){
                	max = value.datum().getMax();
                }
            }
            mean = ttcount * 1.0 / sum;
            var = ssq * 1.0 / sum - mean * mean;
            // Emit the total count for the word.
            WordStatisticsData.Builder builder = WordStatisticsData.newBuilder();
            builder.setCount(sum);
            builder.setTtcount(ttcount);
            builder.setMin(min);
            builder.setMax(max);
            builder.setSsq(ssq);
            builder.setMean(mean);
            builder.setVar(var);
            context.write(key, new AvroValue<WordStatisticsData>(builder.build()));
        }
    }

    /**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordCountA <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();

        // Use this JAR first in the classpath (We also set a bootstrap script in AWS)
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        Job job = Job.getInstance(conf, "WordCountA");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(WordStatistics.class);

        // Specify the Map
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(WordStatisticsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        AvroJob.setMapOutputValueSchema(job, WordStatisticsData.getClassSchema());

        // Specify the Reduce
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(ReduceClass.class);
        job.setOutputKeyClass(Text.class);
        AvroJob.setOutputValueSchema(job, WordStatisticsData.getClassSchema());

        // Grab the input file and output directory from the command line.
        FileInputFormat.addInputPaths(job, appArgs[0]);
        FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

        // Initiate the map-reduce job, and wait for completion.
        job.waitForCompletion(true);

        return 0;
    }

    /**
     * The main method specifies the characteristics of the map-reduce job
     * by setting values on the Job object, and then initiates the map-reduce
     * job and waits for it to complete.
     */
    public static void main(String[] args) throws Exception {
 //       Utils.printClassPath();
        int res = ToolRunner.run(new WordStatistics(), args);
        System.exit(res);
    }

}