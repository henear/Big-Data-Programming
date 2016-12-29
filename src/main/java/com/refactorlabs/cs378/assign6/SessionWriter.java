package com.refactorlabs.cs378.assign6;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

import org.apache.avro.Schema;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.jobhistory.EventType;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.refactorlabs.cs378.assign6.Event;
import com.refactorlabs.cs378.assign6.EventType;
import com.refactorlabs.cs378.assign6.EventSubtype;
import com.refactorlabs.cs378.assign6.BodyStyle;
import com.refactorlabs.cs378.assign6.CabStyle;
import com.refactorlabs.cs378.assign6.VehicleCondition;
import com.refactorlabs.cs378.assign6.Session;
 
public class SessionWriter extends Configured implements Tool {

    // data set headers from dataSetHeaders.tsv
    public final static String[] dataSetHeaders = new String[]{"user_id", "event_type",	"page",
            "referring_domain", "event_timestamp", "city", "vin",	"vehicle_condition", "year", "make",
            "model", "trim",	"body_style", "cab_style", "price", "mileage",            
            "image_count", "carfax_free_report", "features"};

    public static class MapperClass extends Mapper<LongWritable, Text, Text, AvroValue<Session>> {

        /**
         * Counter group for the mapper.  Individual counters are grouped for the mapper.
         */
        private static final String MAPPER_COUNTER_GROUP = "Mapper Counts";

        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            // get all tokens by splitting them by the tab characters
            String[] tokens = line.split("\\t");

            Session.Builder builder = Session.newBuilder();
            Event event = new Event();

            // first token of each line is user's ID... set User Id
            String userId = tokens[0];
            builder.setUserId(userId);

            // get the event and event subtype
            String[] events = tokens[1].split(" ");

            // use the helper method to set the event and subtype
            eventSetter(events, event);

            Class<?> eventClass = event.getClass();

            // for the rest, start with second token
            for (int index = 2; index < tokens.length; index++) {
                // check for features
                String token = tokens[index];
                if (index == tokens.length - 1) {
                    String[] features = token.split(":");
                    Arrays.sort(features);
                    List<CharSequence> allFeatures = new ArrayList<CharSequence>(Arrays.asList(features));
                    event.setFeatures(allFeatures);
                }
               if(dataSetHeaders[index].equals("event_timestamp")){
                	event.setEventTimestamp(token);
                }
                // year case
                if (dataSetHeaders[index].equals("year")) {
                    int year = Integer.parseInt(token);
                   event.setYear(year);
                }
                //body style
                if(dataSetHeaders[index].equals("body_style")){
                	bodystyleenums(token, event);
                }
                if(dataSetHeaders[index].equals("city")){
                    event.setCity(token);
                }

                //vehicle condition
                if(dataSetHeaders[index].equals("vehicle_condition")){
                	vehicleenums(token, event);
                }
                //cab style
                if(dataSetHeaders[index].equals("cab_style")){
                	cabstyleenums(token, event);
                }
                //vin
                if(dataSetHeaders[index].equals("vin")){
                	event.setVin(token);
                }
                if(dataSetHeaders[index].equals("referring_domain")){
                	event.setReferringDomain(token);
                }
                //page
                if(dataSetHeaders[index].equals("page")){
                	event.setPage(token);
                }
                //make
                if(dataSetHeaders[index].equals("make")){
                	event.setMake(token);
                }//model
                if(dataSetHeaders[index].equals("model")){
                	event.setModel (token);
                }
                //trim
                if(dataSetHeaders[index].equals("trim")){
                	event.setTrim(token);
                }
                // image_count case
                if (dataSetHeaders[index].equals("image_count")) {
                    int imageCount = Integer.parseInt(token);
                    event.setImageCount(imageCount);
                }
                // price case
                if (dataSetHeaders[index].equals("price")) {
                    double price = Double.parseDouble(token);
                    event.setPrice(price);
                }
                if(dataSetHeaders[index].equals("carfax_free_report")){
                	boolean b = Boolean.parseBoolean(token);
                	event.setCarfaxFreeReport(b);
                }
                // mileage case
                if (dataSetHeaders[index].equals("mileage")) {
                    int mileage = Integer.parseInt(token);
                    event.setMileage(mileage);
                }
               else{
                    try {
                        // not a feature, regular data
                        // use the field class to set event
                        Field field = eventClass.getField(dataSetHeaders[index]);
                        field.set(event, token);
                    } catch (IllegalAccessException | NoSuchFieldException | IllegalArgumentException exception) {
                        
                    };               	
                }               
            }
            // make an ArrayList of events to pass on to reducer
            List<Event> eventArrayList = new ArrayList<Event>();
            eventArrayList.add(event);
            builder.setEvents(eventArrayList);
            // the first token
            word.set(userId);
            context.write(word, new AvroValue(builder.build()));
        }
    }
 
    public static void eventSetter(String[] events, Event event){
        if(events[0].equals("change")){
        	EventType e = EventType.valueOf("CHANGE");
        	EventSubtype es = EventSubtype.valueOf("CONTACT_FORM");
            event.setEventType(e);
            event.setEventSubtype(es);
        }
        if(events[0].equals("click")){
        	EventType e = EventType.valueOf("CLICK");
        	EventSubtype es = null;
            if(events[1].equals("alternative")){
                 es = EventSubtype.valueOf("ALTERNATIVE");
            }if(events[1].contains("contact")){
                 es = EventSubtype.valueOf("CONTACT_BUTTON");
            }if(events[1].contains("features")){
                 es = EventSubtype.valueOf("FEATURES");
            }if(events[1].contains("get")){
                 es = EventSubtype.valueOf("GET_DIRECTIONS");
            }if(events[1].contains("vehicle")){
                 es = EventSubtype.valueOf("VEHICLE_HISTORY");
            }
            event.setEventType(e);
            event.setEventSubtype(es);
        }
        if(events[0].equals("display")){
            EventType e = EventType.valueOf("DISPLAY");
            EventSubtype es = EventSubtype.valueOf("ALTERNATIVE");
            event.setEventType(e);
            event.setEventSubtype(es);
        }
        if(events[0].equals("edit")){
            EventType e = EventType.valueOf("EDIT");
            EventSubtype es = EventSubtype.valueOf("CONTACT_FORM");
            event.setEventType(e);
            event.setEventSubtype(es);
        }
        if(events[0].equals("show")){
            EventType e = EventType.valueOf("SHOW");
            EventSubtype es = null;
            
            if(events[1].contains("badge")){
                 es = EventSubtype.valueOf("BADGE_DETAIL");
            }if(events[1].contains("photo")){
                 es = EventSubtype.valueOf("PHOTO_MODAL");
            }
            event.setEventType(e);
            event.setEventSubtype(es);
        }
        if(events[0].equals("visit")){
            EventType e = EventType.valueOf("VISIT");
            EventSubtype es = null;
            if(events[1].equals("alternative")){
                 es = EventSubtype.valueOf("ALTERNATIVE");
            }if(events[1].contains("badges")){
                 es = EventSubtype.valueOf("BADGES");
            }if(events[1].contains("contact")){
                 es = EventSubtype.valueOf("CONTACT_FORM");
            }if(events[1].contains("features")){
                 es = EventSubtype.valueOf("FEATURES");
            }if(events[1].contains("market")){
                 es = EventSubtype.valueOf("MARKET_REPORT");
            }if(events[1].contains("vehicle")){
                 es = EventSubtype.valueOf("VEHICLE_HISTORY");
            }
            event.setEventType(e);
            event.setEventSubtype(es);
        }

    }
    
    public static void bodystyleenums(String token, Event event){
    	
    	if(token.equals("Convertible")){
    		BodyStyle b = BodyStyle.valueOf("CONVERTIBLE");
    		event.setBodyStyle(b);    		
    	}
    	if(token.equals("Coupe")){
    		BodyStyle b = BodyStyle.valueOf("COUPE");
    		event.setBodyStyle(b);    		
    	}
        if(token.equals("Hatchback")){
            BodyStyle b = BodyStyle.valueOf("HATCHBACK");
            event.setBodyStyle(b);
        }
        if(token.equals("Minivan")){
            BodyStyle b = BodyStyle.valueOf("MINIVAN");
            event.setBodyStyle(b);
        }
        if(token.equals("Pickup")){
            BodyStyle b = BodyStyle.valueOf("PICKUP");
            event.setBodyStyle(b);
        }
        if(token.equals("SUV 959")){
            BodyStyle b = BodyStyle.valueOf("SUV959");
            event.setBodyStyle(b);
        }
        if(token.equals("Sedan")){
            BodyStyle b = BodyStyle.valueOf("SEDAN");
            event.setBodyStyle(b);
        }
        if(token.equals("Van")){
            BodyStyle b = BodyStyle.valueOf("VAN");
            event.setBodyStyle(b);
        }
        if(token.equals("Wagon")){
            BodyStyle b = BodyStyle.valueOf("WAGON");
            event.setBodyStyle(b);
        }        
    }
    
    public static void vehicleenums(String token, Event event){
       if(token.equals("New")){
           VehicleCondition v = VehicleCondition.valueOf("NEW");
           event.setVehicleCondition(v);
       }
       if(token.equals("Used")){
           VehicleCondition v = VehicleCondition.valueOf("USED");
           event.setVehicleCondition(v);
       }   	
    }
    
    public static void cabstyleenums(String token, Event event){
        if(token.equals("Crew Cab")){
            CabStyle c = CabStyle.valueOf("CREW_CAB");
            event.setCabStyle(c);
        }
        else if(token.equals("Extended Cab")){
            CabStyle c = CabStyle.valueOf("EXTENDED_CAB");
            event.setCabStyle(c);
        }
        else if(token.equals("Regular Cab")){
            CabStyle c = CabStyle.valueOf("REGULAR_CAB");
            event.setCabStyle(c);
        }
        else{
            
            event.setCabStyle(null);
        }

    }
    
    /**
     * The Reduce class for WordStatistics. Extends class Reducer, provided by Hadoop.
     */
    public static class ReduceClass extends Reducer<Text, AvroValue<Session>, AvroKey<CharSequence>,
            AvroValue<Session>> {

        @Override
        public void reduce(Text key, Iterable<AvroValue<Session>> values, Context context)
                throws IOException, InterruptedException {

            Session.Builder builder = Session.newBuilder();
            // set user ID
            builder.setUserId(key.toString());
            // ArrayList of events from the Mapper Class
            ArrayList<Event> allEvents = new ArrayList<Event>();

            // iterate through the given Iterable
            for (AvroValue<Session> value : values) {
                Session session = value.datum();
                // add to our ArrayList
                if(!allEvents.contains(session.events.get(0))){
                allEvents.add(session.events.get(0));
                }
               
            }
            Collections.sort(allEvents, new myComparator());
            builder.setEvents(allEvents);

            // output for the reducer class
            context.write(new AvroKey(key.toString()), new AvroValue(builder.build()));
        }

    }
    
    public static class myComparator implements Comparator<Event>{
        public int compare(Event e1, Event e2){
            String s1 = e1.getEventTimestamp().toString();
            String s2 = e2.getEventTimestamp().toString();
            if(s1!=s2){
            	return s1.compareTo(s2);
            }else{
            	s1 = e1.getEventType().toString();
            	s2 = e2.getEventType().toString();
            	return s1.compareTo(s2);
            }
            
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

        Job job = Job.getInstance(conf, "UserSession");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(SessionWriter.class);
        conf.set("mapreduce.user.classpath.first", "true");

        // Specify the Map
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        AvroJob.setMapOutputValueSchema(job, Session.getClassSchema());

        // Specify the Reduce
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job.setReducerClass(ReduceClass.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job, Session.getClassSchema());

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
        int res = ToolRunner.run(new Configuration(), new SessionWriter(), args);
        System.exit(res);
    }
    
}
