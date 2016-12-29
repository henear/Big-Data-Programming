package com.refactorlabs.cs378.assign4;

import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Map class for various WordCount examples that use the AVRO generated class WordCountData.
 */
public class WordStatisticsMapper extends Mapper<LongWritable, Text, Text, AvroValue<WordStatisticsData>> {

    /**
     * Local variable "word" will contain the word identified in the input.
     */
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
                line = line.toLowerCase();
        StringTokenizer tokenizer = new StringTokenizer(line);
        String token;
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


            WordStatisticsData.Builder builder = WordStatisticsData.newBuilder();

	    for (Map.Entry<String, Integer> entry : mymap.entrySet()) {
	      	int count = entry.getValue().intValue();
			word.set(entry.getKey());

			builder.setCount(1L);
			builder.setTtcount(count);
			builder.setSsq(count* count);
			builder.setMin(count);
			builder.setMax(count);
			builder.setMean(0.0);
			builder.setVar(0.0);
			context.write(word, new AvroValue<WordStatisticsData>(builder.build()));
			context.getCounter("Count map", "Output ").increment(1L);
		}        
    }
}
