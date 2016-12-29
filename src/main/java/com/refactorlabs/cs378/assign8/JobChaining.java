package com.refactorlabs.cs378.assign8;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.refactorlabs.cs378.assign7.*;

public class JobChaining extends Configured implements Tool {
    
    public static final String SUBMITTER = SessionType.SUBMITTER.getText();
    public static final String CLICKER = SessionType.CLICKER.getText();
    public static final String SHOWER = SessionType.SHOWER.getText();
    public static final String VISITOR = SessionType.VISITOR.getText();
    public static abstract class JobChainingMapper extends Mapper<AvroKey<CharSequence>, AvroValue<Session>,
    AvroKey<EventSubtypeStatisticsKey>, AvroValue<EventSubtypeStatisticsData>> {
        
        // to be used by submitter, shower, visitor, clicker mapper
        public abstract String sessionTypeString();
        
        @Override
        public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
        throws IOException, InterruptedException {
            Session session = value.datum();
            List<Event> el= session.getEvents();
            HashMap<EventSubtype, Integer> eventSubMap = new HashMap<>();
            int ct ;
            int tt = 0;
            int temp = 0;
            if(el.size()<=100){
                // iterate through session's events
                for (Event event : session.getEvents()) {
                    EventSubtype eventSub = event.getEventSubtype();
                    Integer count = eventSubMap.get(eventSub);
                    if (count != null) {
                        eventSubMap.put(eventSub, count.intValue() + 1);
                    }else{
                        eventSubMap.put(eventSub, 1);                       
                    }
                }
                
                // iterate through our event subtype HashMap and pass on to reducer
                for (EventSubtype subtype : EventSubtype.values()) {
                	ct = 0;
                    EventSubtypeStatisticsKey.Builder onereducerKey = EventSubtypeStatisticsKey.newBuilder();
                    EventSubtypeStatisticsData.Builder onereducerData = EventSubtypeStatisticsData.newBuilder();
                    
                    // set the the avro value for the reducer
                    onereducerData.setSessionCount(1);
                    Integer count = eventSubMap.get(subtype);
                    
                    if (count != null){
                        ct = count.intValue();
                    }
                    onereducerData.setTotalCount(ct);
                    onereducerData.setSumOfSquares(ct * ct);
                    
                    // set the key
                    onereducerKey.setSessionType(sessionTypeString());
                    onereducerKey.setEventSubtype(subtype.toString());
                    
                    // write to the reducer
                    context.write(new AvroKey<EventSubtypeStatisticsKey>(onereducerKey.build()), new AvroValue<EventSubtypeStatisticsData>(onereducerData.build()));
                }
                EventSubtypeStatisticsKey.Builder anyreducerKey = EventSubtypeStatisticsKey.newBuilder();
                EventSubtypeStatisticsData.Builder anyreducerData = EventSubtypeStatisticsData.newBuilder();
                for (EventSubtype subtype : EventSubtype.values()) {
                    temp = 0;
                    // set the the avro value for the reducer
                    anyreducerData.setSessionCount(1);
                    Integer count = eventSubMap.get(subtype);
                    
                    if (count != null){
                        temp = count.intValue();
                    }
                    	tt += temp;
                }
                anyreducerData.setTotalCount(tt);
                anyreducerData.setSumOfSquares(tt * tt);
                
                // set the key
                anyreducerKey.setSessionType(sessionTypeString());
                anyreducerKey.setEventSubtype("any");
                context.write(new AvroKey<EventSubtypeStatisticsKey>(anyreducerKey.build()), new AvroValue<EventSubtypeStatisticsData>(anyreducerData.build()));
            }
        }
    }
    
    // mapper for the submitter sessions
    public static class SubmitterMapper extends JobChainingMapper {
        @Override
        public String sessionTypeString() {
            return SUBMITTER;
        }
    }
    
    // mapper for the shower sessions
    public static class ShowerMapper extends JobChainingMapper {
        @Override
        public String sessionTypeString() {
            return SHOWER;
        }
    }
    
    // mapper for the clicker sessions
    public static class ClickerMapper extends JobChainingMapper {
        @Override
        public String sessionTypeString() {
            return CLICKER;
        }
    }
    
    // mapper for the visitor sessions
    public static class VisitorMapper extends JobChainingMapper {
        @Override
        public String sessionTypeString() {
            return VISITOR;
        }
    }
    
    //Reducer class to compute the statistics and finalize output
     
    public static class JobChainingReduceClass extends Reducer<AvroKey<EventSubtypeStatisticsKey>, AvroValue<EventSubtypeStatisticsData>, AvroKey<EventSubtypeStatisticsKey>, AvroValue<EventSubtypeStatisticsData>> {
        
        public void reduce(AvroKey<EventSubtypeStatisticsKey> key, Iterable<AvroValue<EventSubtypeStatisticsData>> values, Context context)
        throws IOException, InterruptedException {
            
            int sesCount = 0;
            int subCount = 0;
            int ssq = 0;
            double mean = 0.0;
            double var = 0.0;
            
            // gather all counts
            for (AvroValue<EventSubtypeStatisticsData> builder : values) {
                sesCount += builder.datum().getSessionCount();
                subCount += builder.datum().getTotalCount();
                ssq += builder.datum().getSumOfSquares();
            }
 
            EventSubtypeStatisticsData.Builder reducerOutput = EventSubtypeStatisticsData.newBuilder();
            // set the session count, total count, and sum of squares
            reducerOutput.setSessionCount(sesCount);
            reducerOutput.setTotalCount(subCount);
            reducerOutput.setSumOfSquares(ssq);
            
            // compute the mean and variance
            mean = 1.0* subCount / sesCount;
            var = 1.0* ssq / sesCount - mean * mean;
            reducerOutput.setMean(mean);
            reducerOutput.setVariance(var);
            context.write(key, new AvroValue<EventSubtypeStatisticsData>(reducerOutput.build()));                       
        }
    }
    
    // The run() method is called (indirectly) from main(), and contains all the job
    // setup and configuration.
     
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: JobChaining <input path> <output path>");
            return -1;
        }
        
        Configuration conf = getConf();
        
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
        
        // Filter and Bin Job
        Job filterAndBinJob = Job.getInstance(conf, "MultipleOutputFilter");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        // Identify the JAR file to replicate to all machines.
        filterAndBinJob.setJarByClass(JobChaining.class);
        
        // Specify the Mappers
        filterAndBinJob.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(filterAndBinJob, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(filterAndBinJob, Session.getClassSchema());
        filterAndBinJob.setMapperClass(MultipleOutputFilter.MapClass.class);
        
        AvroJob.setOutputKeySchema(filterAndBinJob, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(filterAndBinJob, Session.getClassSchema());
        filterAndBinJob.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        
        for (SessionType sessionType : SessionType.values()) {
            AvroMultipleOutputs.addNamedOutput(filterAndBinJob, sessionType.getText(), AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.STRING), Session.getClassSchema());
        }
        MultipleOutputs.setCountersEnabled(filterAndBinJob, true);
        
        // Specify the Reducer
        filterAndBinJob.setNumReduceTasks(0);
        
        // Grab the input file and output directory from the command line.
        FileInputFormat.setInputPaths(filterAndBinJob, appArgs[0]);
        FileOutputFormat.setOutputPath(filterAndBinJob, new Path(appArgs[1]));
        
        // initiate the map-reduce job, and wait for completion
        filterAndBinJob.waitForCompletion(true);
        
        // create jobs for the submitter, sharer, and clicker bin
        Job ShowSubEventStatsJob = Job.getInstance(conf, "ShowSubeventStats");
        ShowSubEventStatsJob.setJarByClass(JobChaining.class);
        // Specify the Mapper
        ShowSubEventStatsJob.setInputFormatClass(AvroKeyValueInputFormat.class);
        ShowSubEventStatsJob.setMapperClass(ShowerMapper.class);
        AvroJob.setInputKeySchema(ShowSubEventStatsJob, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(ShowSubEventStatsJob, Session.getClassSchema());
        AvroJob.setMapOutputKeySchema(ShowSubEventStatsJob, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setMapOutputValueSchema(ShowSubEventStatsJob, EventSubtypeStatisticsData.getClassSchema());
        // Specity the reducer
        ShowSubEventStatsJob.setReducerClass(JobChainingReduceClass.class);
        AvroJob.setOutputKeySchema(ShowSubEventStatsJob, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setOutputValueSchema(ShowSubEventStatsJob, EventSubtypeStatisticsData.getClassSchema());
        ShowSubEventStatsJob.setOutputFormatClass(AvroKeyValueOutputFormat.class);    
        // Specify the combiner
        ShowSubEventStatsJob.setCombinerClass(JobChainingReduceClass.class);
        ShowSubEventStatsJob.setNumReduceTasks(1);
        FileInputFormat.addInputPath(ShowSubEventStatsJob, new Path(appArgs[1] + "/" + SessionType.SHOWER.getText() + "-m-*.avro"));
        FileOutputFormat.setOutputPath(ShowSubEventStatsJob, new Path(appArgs[1] + "/" + SessionType.SHOWER.getText() + "Stats"));
        ShowSubEventStatsJob.submit();
        
        // create jobs for the submitter, sharer, and clicker bin
        Job submitterSubeventStatsJob = Job.getInstance(conf, "SubmitterSubeventStats");
        submitterSubeventStatsJob.setJarByClass(JobChaining.class);
        // Specify the Mapper
        submitterSubeventStatsJob.setInputFormatClass(AvroKeyValueInputFormat.class);
        submitterSubeventStatsJob.setMapperClass(SubmitterMapper.class);
        AvroJob.setInputKeySchema(submitterSubeventStatsJob, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(submitterSubeventStatsJob, Session.getClassSchema());
        AvroJob.setMapOutputKeySchema(submitterSubeventStatsJob, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setMapOutputValueSchema(submitterSubeventStatsJob, EventSubtypeStatisticsData.getClassSchema());
        // Specity the reducer
        submitterSubeventStatsJob.setReducerClass(JobChainingReduceClass.class);
        AvroJob.setOutputKeySchema(submitterSubeventStatsJob, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setOutputValueSchema(submitterSubeventStatsJob, EventSubtypeStatisticsData.getClassSchema());
        submitterSubeventStatsJob.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        // Specify the combiner
        submitterSubeventStatsJob.setCombinerClass(JobChainingReduceClass.class);
        submitterSubeventStatsJob.setNumReduceTasks(1);
        FileInputFormat.addInputPath(submitterSubeventStatsJob, new Path(appArgs[1] + "/" + SessionType.SUBMITTER.getText() + "-m-*.avro"));
        FileOutputFormat.setOutputPath(submitterSubeventStatsJob, new Path(appArgs[1] + "/" + SessionType.SUBMITTER.getText() + "Stats"));
        submitterSubeventStatsJob.submit();
        
        // create jobs for the submitter, sharer, and clicker bin
        Job clickerSubeventStatsJob = Job.getInstance(conf, "ClickerSubeventStats");
        clickerSubeventStatsJob.setJarByClass(JobChaining.class);
        // Specify the Mapper
        clickerSubeventStatsJob.setInputFormatClass(AvroKeyValueInputFormat.class);
        clickerSubeventStatsJob.setMapperClass(ClickerMapper.class);
        AvroJob.setInputKeySchema(clickerSubeventStatsJob, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(clickerSubeventStatsJob, Session.getClassSchema());
        AvroJob.setMapOutputKeySchema(clickerSubeventStatsJob, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setMapOutputValueSchema(clickerSubeventStatsJob, EventSubtypeStatisticsData.getClassSchema());
        // Specity the reducer
        clickerSubeventStatsJob.setReducerClass(JobChainingReduceClass.class);
        AvroJob.setOutputKeySchema(clickerSubeventStatsJob, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setOutputValueSchema(clickerSubeventStatsJob, EventSubtypeStatisticsData.getClassSchema());
        clickerSubeventStatsJob.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        // Specify the combiner
        clickerSubeventStatsJob.setCombinerClass(JobChainingReduceClass.class);
        clickerSubeventStatsJob.setNumReduceTasks(1);
        FileInputFormat.addInputPath(clickerSubeventStatsJob, new Path(appArgs[1] + "/" + SessionType.CLICKER.getText() + "-m-*.avro"));
        FileOutputFormat.setOutputPath(clickerSubeventStatsJob, new Path(appArgs[1] + "/" + SessionType.CLICKER.getText() + "Stats"));
        clickerSubeventStatsJob.submit();
        
        // create jobs for the submitter, sharer, and clicker bin
        Job visitorSubeventStatsJob = Job.getInstance(conf, "VisitorSubeventStats");
        visitorSubeventStatsJob.setJarByClass(JobChaining.class);
        // Specify the Mapper
        visitorSubeventStatsJob.setInputFormatClass(AvroKeyValueInputFormat.class);
        visitorSubeventStatsJob.setMapperClass(VisitorMapper.class);
        AvroJob.setInputKeySchema(visitorSubeventStatsJob, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(visitorSubeventStatsJob, Session.getClassSchema());
        AvroJob.setMapOutputKeySchema(visitorSubeventStatsJob, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setMapOutputValueSchema(visitorSubeventStatsJob, EventSubtypeStatisticsData.getClassSchema());
        // Specity the reducer
        visitorSubeventStatsJob.setReducerClass(JobChainingReduceClass.class);
        AvroJob.setOutputKeySchema(visitorSubeventStatsJob, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setOutputValueSchema(visitorSubeventStatsJob, EventSubtypeStatisticsData.getClassSchema());
        visitorSubeventStatsJob.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        // Specify the combiner
        visitorSubeventStatsJob.setCombinerClass(JobChainingReduceClass.class);
        visitorSubeventStatsJob.setNumReduceTasks(1);
        FileInputFormat.addInputPath(visitorSubeventStatsJob, new Path(appArgs[1] + "/" + SessionType.VISITOR.getText() + "-m-*.avro"));
        FileOutputFormat.setOutputPath(visitorSubeventStatsJob, new Path(appArgs[1] + "/" + SessionType.VISITOR.getText() + "Stats"));
        visitorSubeventStatsJob.submit();
        
        while ( !submitterSubeventStatsJob.isComplete()|| !ShowSubEventStatsJob.isComplete() ||!clickerSubeventStatsJob.isComplete()||!visitorSubeventStatsJob.isComplete()) {
            Thread.sleep(500);
        }
        
        // aggregate job
        Job aggregateClickStatsJob = Job.getInstance(conf, "AggregateSubeventStats");
        aggregateClickStatsJob.setJarByClass(JobChaining.class);
        aggregateClickStatsJob.setInputFormatClass(AvroKeyValueInputFormat.class);
        aggregateClickStatsJob.setMapperClass(Mapper.class);
        AvroJob.setInputKeySchema(aggregateClickStatsJob, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setInputValueSchema(aggregateClickStatsJob, EventSubtypeStatisticsData.getClassSchema());
        AvroJob.setMapOutputKeySchema(aggregateClickStatsJob, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setMapOutputValueSchema(aggregateClickStatsJob, EventSubtypeStatisticsData.getClassSchema());
        aggregateClickStatsJob.setCombinerClass(JobChainingReduceClass.class);
        aggregateClickStatsJob.setReducerClass(JobChainingReduceClass.class);
        AvroJob.setOutputKeySchema(aggregateClickStatsJob, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setOutputValueSchema(aggregateClickStatsJob, EventSubtypeStatisticsData.getClassSchema());
        aggregateClickStatsJob.setOutputFormatClass(TextOutputFormat.class);
        aggregateClickStatsJob.setNumReduceTasks(1);
        
        FileInputFormat.addInputPath(aggregateClickStatsJob, inputPath(appArgs[1], SHOWER));
        FileInputFormat.addInputPath(aggregateClickStatsJob, inputPath(appArgs[1], SUBMITTER));
        FileInputFormat.addInputPath(aggregateClickStatsJob, inputPath(appArgs[1], CLICKER));
        FileInputFormat.addInputPath(aggregateClickStatsJob, inputPath(appArgs[1], VISITOR));
        FileOutputFormat.setOutputPath(aggregateClickStatsJob, new Path(outputDirectory(appArgs[1], "aggregate")));
        
        aggregateClickStatsJob.waitForCompletion(true);
        
        return 0;
    }
    
    // helper method to read in the input path
    public static Path inputPath(String baseInputPath, String header) {
        return new Path(baseInputPath + "/" + header + "Stats" + "/" + "part-r-*.avro");
    }
    
    // helper method to get the output directory
    public static String outputDirectory(String InputPath, String prefix) {
        return InputPath + "/" + prefix + "Stats";
    }
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new JobChaining(), args);
        System.exit(res);
    }
}
