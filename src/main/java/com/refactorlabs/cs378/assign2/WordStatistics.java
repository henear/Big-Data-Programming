package com.refactorlabs.cs378.assign2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
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
import java.util.*;

/**
 * Example MapReduce program that performs word count.
 *
 * @author David Franke (dfranke@cs.utexas.edu)
 */
public class WordStatistics {

	/**
	 * The Map class for word count.  Extends class Mapper, provided by Hadoop.
	 * This class defines the map() function for the word count example.
	 */
	public static class MapClass extends Mapper<LongWritable, Text, Text, WordStatisticsWritable> {

		/**
		 * Local variable "word" will contain the word identified in the input.
		 * The Hadoop Text object is mutable, so we can reuse the same object and
		 * simply reset its value as each word in the input is encountered.
		 */
		private Text word = new Text();

		private IntWritable[] mydata = new IntWritable[3];
		private WordStatisticsWritable output = new WordStatisticsWritable();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			line = change(line);
			StringTokenizer tokenizer = new StringTokenizer(line);
			String token;
			// For each word in the input line, add to hashMap, if not in map, add it.
			HashMap<String, Integer> mymap = new HashMap<String, Integer>();
			while (tokenizer.hasMoreTokens()) {
				token = tokenizer.nextToken();
				
				if(mymap.get(token) != null){
					int temp = (mymap.get(token)).intValue();
					temp ++;
					mymap.put(token, new Integer(temp));
				}else{
					mymap.put(token, new Integer(1));					
				}
			}

			// Write out all of the values (count) of the keys (words) in map
		    for (Map.Entry<String, Integer> entry : mymap.entrySet()) {
		      	int count = entry.getValue().intValue();
				word.set(entry.getKey());
				mydata[0] = new IntWritable(1);
				mydata[1] = new IntWritable(count);
				mydata[2] = new IntWritable(count * count);
				output.set(mydata);
				context.write(word, output);
				context.getCounter("Count map", "Output ").increment(1L);
			}
		}
	}

	public static String change(String line){
		String punc = ",.;:?\"'=_!()";
		line = line.toLowerCase();
		
		if(line.contains("--")){
			line = line.replace("--", " ");
		}
		if(line.contains("[")){
			line = line.replace("[", " [");
		}
		
		int l = line.length();
		
		for(int i = 0; i < l; i++){
			if(punc.contains(line.substring(i, i+1))){
				line = line.replace(line.substring(i, i+1), " ");
			}
		}
		
		
		return line;
	}
	
	//Combiners are here
	public static class CombinerClass extends Reducer<Text, WordStatisticsWritable, Text, WordStatisticsWritable>{
		
		private IntWritable[] data = new IntWritable[3];
		private WordStatisticsWritable output = new WordStatisticsWritable();

		@Override
		public void reduce(Text key, Iterable<WordStatisticsWritable> values, Context context) throws IOException, InterruptedException {

			int paras = 0;
			int count = 0;
			int scount = 0;
			Writable[] temp = new Writable[3];

			for( WordStatisticsWritable value : values){
				temp = value.get();
				paras += ((IntWritable)temp[0]).get();
				count += ((IntWritable)temp[1]).get();
				scount += ((IntWritable)temp[2]).get();
			}

			data[0] = new IntWritable(paras);
			data[1] = new IntWritable(count);
			data[2] = new IntWritable(scount);
			output.set(data);
			context.write(key, output);

		}
	}
	/**
	 * The Reduce class for word count.  Extends class Reducer, provided by Hadoop.
	 * This class defines the reduce() function for the word count example.
	 */
	public static class ReduceClass extends Reducer<Text, WordStatisticsWritable, Text, String> {

		private DoubleWritable[] data = new DoubleWritable[2];
		
		@Override
		public void reduce(Text key, Iterable<WordStatisticsWritable> values, Context context) throws IOException, InterruptedException {

				int para = 0;
				int count = 0;
				int scount = 0;
				Writable[] temp;
				for( WordStatisticsWritable value : values){
					temp = value.get();
					para += ((IntWritable)temp[0]).get();
					count += ((IntWritable)temp[1]).get();
					scount += ((IntWritable)temp[2]).get();
				}
				double mean = 1.0 * count/para;
				double variance = (1.0 * scount/para)-(mean*mean);

				data[0] = new DoubleWritable(mean);
				data[1] = new DoubleWritable(variance);
				String s = para + ", " + data[0] + ", " + data[1];
				context.write(key, s);

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
		Job job = new Job(conf, "Word Statistics");
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(WordStatistics.class);

		// Set the output key and value types (for map and reduce).
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(WordStatisticsWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(WordStatisticsWritable.class);

		// Set the map, combiner, and reduce classes.
		job.setMapperClass(MapClass.class);
		job.setCombinerClass(CombinerClass.class);
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