package com.refactorlabs.cs378.assign3;

// Imports
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;
public class InvertedIndex extends Configured implements Tool {

	// Map and Reduce classes


	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

		/**
		 * Local variable "word" will contain the word identified in the input.
		 * The Hadoop Text object is mutable, so we can reuse the same object and
		 * simply reset its value as each word in the input is encountered.
		 */

		private Text word = new Text();

		private Text mydata = new Text();
		private Text outputValue = new Text();
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String temp;
			String line = value.toString();
			if(line.length()!=0){
				String[] tempp = line.split(" ");
				temp = tempp[0];
				String line2 = change(line);
				StringTokenizer tokenizer = new StringTokenizer(line2);
				String token;

				// For each word in the input line, add to hashMap, if not in map, add it.
				HashMap<String, String> mymap = new HashMap<String, String>();
				while (tokenizer.hasMoreTokens()) {
					token = tokenizer.nextToken();
					if(!mymap.containsKey(token)){
						mymap.put(token, temp);
					}
				}

				// Write out all of the values (count) of the keys (words) in map
				for (Map.Entry<String, String> entry : mymap.entrySet()) {
					mydata = new Text(entry.getValue());
					word.set(entry.getKey());
					outputValue.set(mydata);
					context.write(word, outputValue);
					context.getCounter("Mapper Counts", "Output Words").increment(1L);
				}
			}
		}
	}	

	public static class CombineClass extends Reducer<Text, Text, Text, Text>{

		private Text output = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder(); 
			for( Text value : values){
				sb.append(value.toString());
				sb.append(",");
			}
			String[] s = sb.toString().split(",");
			s = customSort(s);
			StringBuilder mys = new StringBuilder();
			for(String str: s){
				String temp = str + ",";
				mys.append(temp);
			}
			String temp = mys.toString().substring(0, mys.length()-1);
			output = new Text(temp);
			//output.set(data);
			context.write(key, output);
		}
	}
	public static class ReduceClass extends Reducer<Text, Text, Text, Text>{

		private Text output = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder(); 
			for( Text value : values){
				sb.append(value.toString());
				sb.append(",");
			}
			String[] s = sb.toString().split(",");
			s = customSort(s);
			StringBuilder mys = new StringBuilder();
			for(String str: s){
				String temp = str + ",";
				mys.append(temp);
			}
			String temp = mys.toString().substring(0, mys.length()-1);
			output = new Text(temp);
			//output.set(data);
			context.write(key, output);


		}
	}

	public static String change(String line){
		int i = 0;
		while(line.charAt(i)!= ' '){
			i++;
		}
		line = line.substring(i);
		String punc = ",.;:?\"'=_!()";
		line = line.toLowerCase();

		if(line.contains("--")){
			line = line.replace("--", " ");
		}
		if(line.contains("[")){
			line = line.replace("[", " [");
		}

		int l = line.length();

		for(i = 0; i < l; i++){
			if(punc.contains(line.substring(i, i+1))){
				line = line.replace(line.substring(i, i+1), " ");
			}
		}


		return line.substring(0, 1).toUpperCase()+ line.substring(1);
	}

	public static String[] customSort(String[] s){
		int l = s.length;
		String temp = "";
		String[] s1 = new String[3];
		String[] s2 = new String[3];
		for(int i = 0;i < l; i++){
			for(int j = i;j < l; j++){
				s1 = s[i].split(":");
				s2 = s[j].split(":");
				if(s1[0].compareTo(s2[0])>0){
					temp = s[i];
					s[i] = s[j];
					s[j] = temp;
				}else if(s1[0].compareTo(s2[0])==0){
					int a1 = Integer.parseInt(s1[1]);
					int a2 = Integer.parseInt(s2[1]);
					if(a1>a2){
						temp = s[i];
						s[i] = s[j];
						s[j] = temp;
					}else if(s1[1].compareTo(s2[1])==0){
						int b1 = Integer.parseInt(s1[2]);
						int b2 = Integer.parseInt(s2[2]);
						if(b1>b2){
							temp = s[i];
							s[i] = s[j];
							s[j] = temp;
						}
					}
				}

			}
		}
		return s;
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "InvertedIndex");
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(InvertedIndex.class);

		// Set the output key and value types (for map and reduce).
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Set the map, combiner, and reduce classes.
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
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new InvertedIndex(), args);
		System.exit(res);
	}

}
