package com.refactorlabs.cs378.assign6;

import java.io.IOException;
import java.util.*;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

public class MultipleInputjoin extends Configured implements Tool {
    public static class SessionMapperClass
            extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, Text, AvroValue<VinImpressionCounts>> {
        private Text word = new Text();

        @Override
        public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
                throws IOException, InterruptedException {

            Session session = value.datum();

            HashMap<String, Long> uniqueUsers = new HashMap<>();
            HashMap<String, HashMap<CharSequence, Long>> userClicks = new HashMap<>();
            Set<String> contact = new HashSet<>();
                     
            // look through events of the Session Avro output file
            for (Event event : session.getEvents()) {
                String vin = event.getVin().toString();
                // store unique users since HashMaps don't allow duplicate keys
                uniqueUsers.put(vin, 1L);
              
                if (event.getEventType() == EventType.CLICK) {                    
                    if (!userClicks.containsKey(vin)) {                        
                    	HashMap<CharSequence, Long> thisClick = new HashMap<>();
                        thisClick.put(event.getEventSubtype().name().toString(), 1L);
                        userClicks.put(vin, thisClick);
                    }
                    else {
                    	HashMap<CharSequence, Long> thisClick = userClicks.get(vin);
                        thisClick.put(event.getEventSubtype().name().toString(), 1L);
                    }
                }
                else if (event.getEventType() == EventType.EDIT){
                    contact.add(vin);
                }
            }

            for (String vin : uniqueUsers.keySet()) {
                VinImpressionCounts.Builder vinImpressionBuilder = VinImpressionCounts.newBuilder();
                vinImpressionBuilder.setUniqueUsers(1L);
                if (contact.contains(vin)){
                    vinImpressionBuilder.setEditContactForm(1L);
                }
                if (userClicks.containsKey(vin)){
                    vinImpressionBuilder.setClicks(userClicks.get(vin));              
                }
                word.set(vin);
                context.write(word, new AvroValue(vinImpressionBuilder.build()));
            }
        }
    }

    //Mapper to read in the VIN dataSet7VinCounts.csv file
    //Unique users either viewed search results page (SRP) or a vehicle detail page (VDP)
     
    public static class VINMapperClass extends Mapper<LongWritable, Text, Text, AvroValue<VinImpressionCounts>> {

        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String split[] = line.split(",");

            // the current VIN
            word.set(split[0]);
            
            // the count for the current VIN
            Long count = Long.parseLong(split[2]);

            VinImpressionCounts.Builder vinBuilder = VinImpressionCounts.newBuilder();
            // SRP
            if (split[1].equals("SRP"))
                vinBuilder.setMarketplaceSrps(count);
            if (split[1].equals("VDP")){
                vinBuilder.setMarketplaceVdps(count);
            }
            // pass on to reducer
            context.write(word, new AvroValue<>(vinBuilder.build()));
        }
    }

    //shares the Reduce Class for combining VinImpressionsCounts instances
     
    public static class CombinerClass
            extends Reducer<Text, AvroValue<VinImpressionCounts>, Text, AvroValue<VinImpressionCounts>> {
        private Text word = new Text();
        public static VinImpressionCounts.Builder LeftOuterJoinCombiner(Text key, Iterable<AvroValue<VinImpressionCounts>> values)
                throws IOException, InterruptedException {
            VinImpressionCounts.Builder vinBuilder = VinImpressionCounts.newBuilder();
            // since CLICK needs to be held in a map
            HashMap<CharSequence, Long> clickM = new HashMap<>();

            for (AvroValue<VinImpressionCounts> value : values) {
                // for each instance of VinImpressionCounts
                VinImpressionCounts current = value.datum();

                // get other builder's click map
                Map<CharSequence, Long> oClickM = current.getClicks();
                if (oClickM != null) { // other click map exists, add elements
                    for (Map.Entry<CharSequence, Long> clickEntry : oClickM.entrySet()) {
                        CharSequence clickKey = clickEntry.getKey();
                        if (clickM.containsKey(clickKey))
                            clickM.put(clickKey, clickM.get(clickKey) + oClickM.get(clickKey));
                        else
                            clickM.put(clickKey, oClickM.get(clickKey));
                    }
                }

                // now take care of unique users, show_badge_details, edit_contact_form, submit_contact_form, srps, vdps
                vinBuilder.setUniqueUsers(vinBuilder.getUniqueUsers() + current.getUniqueUsers());
                vinBuilder.setEditContactForm(vinBuilder.getEditContactForm()+ current.getEditContactForm());
                vinBuilder.setMarketplaceVdps(vinBuilder.getMarketplaceVdps()+ current.getMarketplaceVdps());
                vinBuilder.setMarketplaceSrps(vinBuilder.getMarketplaceSrps()+ current.getMarketplaceSrps());
            }
            vinBuilder.setClicks(clickM);
            return vinBuilder;
        }
    }

    // left outer join
    
    public static class ReduceClass
            extends Reducer<Text, AvroValue<VinImpressionCounts>, Text, AvroValue<VinImpressionCounts>> {

        private Text word = new Text();

        @Override
        public void reduce(Text key, Iterable<AvroValue<VinImpressionCounts>> values, Context context)
                throws IOException, InterruptedException {
            VinImpressionCounts.Builder vinImpressionBuilder = CombinerClass.LeftOuterJoinCombiner(key, values);
            // only write to output if there is at least 1 unique user
            if (vinImpressionBuilder.getUniqueUsers() != 0)
                context.write(key, new AvroValue(vinImpressionBuilder.build()));
        }
    }

    /**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: MultipleInputsJoin <input path> <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();

        // Use this JAR first in the classpath (We also set a bootstrap script in AWS)
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        Job job = Job.getInstance(conf, "MultipleInputsJoin");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(MultipleInputjoin.class);
        conf.set("mapreduce.user.classpath.first", "true");

        // Specify the multiple inputs for mappers.
        MultipleInputs.addInputPath(job, new Path(appArgs[0]), AvroKeyValueInputFormat.class, SessionMapperClass.class);
        MultipleInputs.addInputPath(job, new Path(appArgs[1]), TextInputFormat.class, VINMapperClass.class);

        // Specify input key schema for avro input type.
        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(job, Session.getClassSchema());

        // Specify the Map
        job.setMapOutputKeyClass(Text.class);
        AvroJob.setMapOutputValueSchema(job, VinImpressionCounts.getClassSchema());

        // Set combiner class.
        job.setCombinerClass(CombinerClass.class);

        // Specify the Reduce
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(ReduceClass.class);
        job.setOutputKeyClass(Text.class);
        AvroJob.setOutputValueSchema(job, Session.getClassSchema());
        FileOutputFormat.setOutputPath(job, new Path(appArgs[2]));
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
        int res = ToolRunner.run(new Configuration(), new MultipleInputjoin(), args);
        System.exit(res);
    }
}
