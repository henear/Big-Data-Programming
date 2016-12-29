package com.refactorlabs.cs378.assign11;


import org.apache.spark.Accumulator;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * User session application for Spark.
 */
public class UserSession implements java.io.Serializable{
			
	public static void main(String[] args) {
		//Utils.printClassPath();

		String inputFilename = args[0];
		String outputFilename = args[1];

		// Create a Java Spark context
		SparkConf conf = new SparkConf().setAppName(UserSession.class.getName()).setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load the input data
		JavaRDD<String> input = sc.textFile(inputFilename);

		// Create Accumulators.
		Accumulator<Integer> filteringEventCount = sc.accumulator(0);
		Accumulator<Integer> uniqueEventCount = sc.accumulator(0);
		Accumulator<Integer> sessionCount = sc.accumulator(0);
		Accumulator<Integer> totalshowerCount = sc.accumulator(0);
		Accumulator<Integer> discardshowerCount = sc.accumulator(0);
		Set<Tuple2<String, String>> uniqueSessions = new HashSet<Tuple2<String, String>>();
		Set<String> eventSet = new HashSet<String>();
		
		// Transform into Tuple2(new Tuple2(userId, referringDomain), eventList)
		PairFunction<String,  Tuple2<String,String>, Iterable<Event>> getInputFuction =
				new PairFunction<String, Tuple2<String,String>, Iterable<Event>>() {
			@Override
			public Tuple2<Tuple2<String,String>, Iterable<Event>> call(String line) throws Exception {
				
				String[] specs = line.split("\t");
				Event tempEvent = new Event();
				
				// Store userIds, referringDomains, event specs
				String userID = specs[0];
				String eventStr = specs[1];
				String referDomain = specs[3];
				tempEvent.eventType = eventStr.split(" ")[0];
				tempEvent.eventSubType = eventStr.substring(eventStr.indexOf(" ")+1);
				tempEvent.eventTimestamp = specs[4];
				
				// Create a String for each event, used to remove dulplicate
				String key = specs[0] + tempEvent.toString();
				ArrayList<Event> eventList = new ArrayList<Event>();
				
				//Count unique sessions
				if(uniqueSessions.add(new Tuple2<String, String>(userID, referDomain))){
					sessionCount.add(1);
				}
				// Store unique event into the eventList and count unique events
				if (eventSet.add(key)){
					eventList.add(tempEvent);
					uniqueEventCount.add(1);
				}
				return new Tuple2<Tuple2<String,String>, Iterable<Event>>(new Tuple2<String, String>(userID, referDomain), eventList);
			}
		};

		Function2<Iterable<Event>, Iterable<Event>, Iterable<Event>> sumFunction =
				new Function2<Iterable<Event>, Iterable<Event>, Iterable<Event>> () {
			@Override
			public Iterable<Event> call(Iterable<Event> e1, Iterable<Event> e2)throws Exception {
				
				// Add all events and sort them
				ArrayList<Event> events = new ArrayList<Event>((ArrayList<Event>)e1);
				events.addAll((ArrayList<Event>)e2);
				Collections.sort(events, new Comparator<Event>() {
					
					//sorting by timestamp, use the event type as a secondary sort, event subtype as a tertiary sort
						public int compare (Event e1, Event e2){
							if(e1.eventTimestamp != e2.eventTimestamp){
								return e1.eventTimestamp.compareTo(e2.eventTimestamp);
							}else if(e1.eventType != e2.eventType){
								return e1.eventType.compareTo(e2.eventType);
							}else{
								return e1.eventSubType.compareTo(e2.eventSubType);
							}
						}
				});
				return events;
			}
		};

		// Filter out some SHOWER sessions.
		Function<Tuple2<Tuple2<String, String>, Iterable<Event>>, Boolean> filterSessionFunction =
				new Function<Tuple2<Tuple2<String, String>, Iterable<Event>>, Boolean> (){
			
			@Override
			public Boolean call(Tuple2<Tuple2<String, String>, Iterable<Event>> allpair)
					throws Exception {
				boolean isshower = false;
				Iterable<Event> temp = allpair._2;
				
				for (Event e: temp){					
					if (e.eventSubType.equals("contact form")||e.eventType.equals("click")){
						isshower = false;
						break;
					}				
					if ((e.eventType.equals("show")) || (e.eventType.equals("display"))){
						isshower = true;
					}
				}
				
				// Count SHOWER sessions and SHOWER sessions after filtering out 90%
				Random rand = new Random();
				if(isshower){
					totalshowerCount.add(1);
					if (rand.nextDouble()<0.9) {
						discardshowerCount.add(1);
						return false;								
					}
					else {
						for (Event e: temp){					
							filteringEventCount.add(1);
						}
						return true;
					}
				}
				else {
					for (Event e: temp){
						filteringEventCount.add(1);
					}
					return true;
				}
			}
		};
		
		JavaPairRDD<Tuple2<String, String>, Iterable<Event>> Unsortevents = input.mapToPair(getInputFuction);
		JavaPairRDD<Tuple2<String, String>, Iterable<Event>> sortedEvents = Unsortevents.reduceByKey(sumFunction);
		JavaPairRDD<Tuple2<String, String>, Iterable<Event>> filtershowerSessions = sortedEvents.filter(filterSessionFunction);
		JavaPairRDD<Tuple2<String, String>, Iterable<Event>> sortSessions = filtershowerSessions.sortByKey(new TupleComparator(),true);
		JavaPairRDD<Tuple2<String, String>, Iterable<Event>> finalResult = sortSessions.partitionBy(new MyPartitioner());
		 
		// Save the word count to a text file (initiates evaluation)
		finalResult.saveAsTextFile(outputFilename);
		
		// Output accumulator results.
		System.out.println("Total number of unique events before filtering: " + uniqueEventCount.value() );
		System.out.println("Total number of unique events after filtering: " + filteringEventCount.value());
		System.out.println("Total number of sessions: " + sessionCount.value());
		System.out.println("Total number of SHOWER sessions: " + totalshowerCount.value() );
		System.out.println("Total number of filtered SHOWER sessions: " + discardshowerCount.value());
		// Shut down the context
		sc.stop();
	}	

	// A comparator used to sort sessions.
	public static class TupleComparator implements Comparator<Tuple2<String, String>>, Serializable {
	    
		// A comparator used to sort sessions first by userID, then by domain
		@Override
	    public int compare(Tuple2<String, String> t1, Tuple2<String, String> t2) {
			if(t1._1!=t2._1){
				return t1._1.compareTo(t2._1);
			}else{
				return t1._2.compareTo(t2._2);
			}
	    }		
	}
	
	private static class Event implements Serializable {
		String eventType;
		String eventSubType;
		String eventTimestamp;
		public String toString() { return "<" + eventType + ":" + eventSubType + "," + eventTimestamp + ">";}
	}
	
	private static class MyPartitioner extends Partitioner{		
				@Override
				public int getPartition(Object key) {
					String temp = ((Tuple2)key)._2().toString();
					return Math.abs(temp.hashCode()) % numPartitions();
				}				

				@Override
				public int numPartitions() {
					return 6;
				}
			}
}
