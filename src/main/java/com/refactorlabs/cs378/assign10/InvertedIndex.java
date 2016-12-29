package com.refactorlabs.cs378.assign10;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

/**
 * InvertedIndex application for Spark.
 */


public class InvertedIndex {
	public static void main(String[] args) {

		String inputFilename = args[0];
		String outputFilename = args[1];

		// Create a Java Spark context
		SparkConf conf = new SparkConf().setAppName(InvertedIndex.class.getName()).setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load the input data
		JavaRDD<String> input = sc.textFile(inputFilename);

		// Split the input into words
		FlatMapFunction<String, String> splitFunction = new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String line) throws Exception {
				List<String> wordList = Lists.newArrayList();
				
				String mytemp = line;
				mytemp = line.replace("\n", "");

				if(!mytemp.equals("")){
					String value = line.split(" ")[0];
					line = change(line);  

					StringTokenizer tokenizer = new StringTokenizer(line);

					HashSet<String> wordSet = new HashSet<>();
					// For each word in the input line, emit that word.
					while (tokenizer.hasMoreTokens()) {
						String s = tokenizer.nextToken();
						String temp = s + "," + value;
						if(wordSet.add(s)){
							wordList.add(temp);
							//tupleList.add(new Tuple2<String, String>(nextWord, Verse))
						}
					}
				}
				return wordList.iterator();
			} 
		};

		// Transform into word and verse
		PairFunction<String, String, String> addVerseFunction =
				new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String s) throws Exception {           	
				return new Tuple2(s.split(",")[0], s.split(",")[1]);

			}
		};

		// Sum the counts
		Function2<String, String, String> sumFunction = new Function2<String, String, String>() {
			@Override
			public String call(String verse1, String verse2) throws Exception {            	
				return verse1 + "," + verse2;                
			}
		};

		Function<String, Iterable<String>> sortVerseFunction = 
				new Function<String, Iterable<String>>() {
			public Iterable<String> call(String listofVerse) throws Exception {
				String[] verseArray = listofVerse.split(",");
				List<String> verseList = Arrays.asList(verseArray);
				Collections.sort(verseList, new myComparator());
				String temp = verseList.toString();
				ArrayList<String> t = new ArrayList<>();
				t.add(temp);
				return t;
			}
		};
		JavaRDD<String> words = input.flatMap(splitFunction);
		JavaPairRDD<String, String> wordsWithVerse = words.mapToPair(addVerseFunction);
		JavaPairRDD<String, String> counts = wordsWithVerse.reduceByKey(sumFunction);
		JavaPairRDD<String, String> count2 =  counts.flatMapValues(sortVerseFunction);    
		JavaPairRDD<String, String> ordered = count2.sortByKey();     
		ordered.saveAsTextFile(outputFilename);
		// Shut down the context
		sc.stop();
	}

	public static String change(String line){
		int i = 0;
		while(line.charAt(i)!= ' '){
			i++;
		}
		line = line.substring(i);
		String punc = ",.;:?\"=_!()";
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

	public static class myComparator implements Comparator<String>{
		public int compare(String verse1, String verse2){
			String[] myverse1 = verse1.split(":");
			String[] myverse2 = verse2.split(":");
			if(myverse1[0]!=myverse2[0]){
				return myverse1[0].compareTo(myverse2[0]);
			}else{
				int a1 = Integer.parseInt(myverse1[1]);
				int a2 = Integer.parseInt(myverse2[1]);
				if(a1!=a2){
					if(a1>a2){
						return 1;
					}else{
						return -1;
					}
				}else{
					int b1 = Integer.parseInt(myverse1[2]);
					int b2 = Integer.parseInt(myverse2[2]);
					if(b1>b2){
						return 1;
					}else{
						return -1;
					}
				}
			}
		}
	}
}
