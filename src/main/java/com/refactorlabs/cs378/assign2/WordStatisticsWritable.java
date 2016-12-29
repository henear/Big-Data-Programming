package com.refactorlabs.cs378.assign2;


import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;

public class WordStatisticsWritable extends ArrayWritable {	
		
        public WordStatisticsWritable() {
                super(IntWritable.class);
        }

        
        public WordStatisticsWritable(IntWritable[] values) {
                super(IntWritable.class, values);
        }
        
        public WordStatisticsWritable(DoubleWritable[] values){        	        		
        		super(DoubleWritable.class, values);
        }
        
        
        
        
}