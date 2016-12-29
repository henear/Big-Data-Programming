package com.refactorlabs.cs378.assign7;

import java.io.IOException;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MultipleOutputFilter extends Configured implements Tool{
	public static final String WRITE_COUNTER = "write counter";
	public static final String SUBMITTER = SessionType.SUBMITTER.getText();
	public static final String CLICKER = SessionType.CLICKER.getText();
	public static final String SHOWER = SessionType.SHOWER.getText();
	public static final String VISITOR = SessionType.VISITOR.getText();
	public static final String DIS_COUNT = "discard session counter";
	public static class MapClass extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, AvroKey<CharSequence>, AvroValue<Session>> {

		private AvroMultipleOutputs multipleOutput;
		public void setup(Context context){
			multipleOutput = new AvroMultipleOutputs(context);
		}
		public void cleanup(Context context) throws InterruptedException, IOException{
			multipleOutput.close();
		}

		public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
				throws IOException, InterruptedException {
			Session session = value.datum();
			List<Event> el= session.getEvents();
			boolean issubmit = false;
			boolean isclick = false;
			boolean isshow = false;
			boolean isvisit = false;
			if(el.size() > 100){
				context.getCounter(DIS_COUNT, "large sessions").increment(1L);
			}else{
				for(Event event : el){
					if (event.getEventType()==EventType.CHANGE||
							(event.getEventType()==EventType.EDIT&&event.getEventSubtype()==EventSubtype.CONTACT_FORM)) {
						issubmit = true;
					}else if(event.getEventType()==EventType.CLICK){
						isclick = true;
					}else if(event.getEventType()==EventType.DISPLAY||event.getEventType()==EventType.SHOW){
						isshow = true;
					}else if(event.getEventType()==EventType.VISIT){
						isvisit = true;
					}
				}
				if(issubmit){
					multipleOutput.write(SUBMITTER, key, value);
				}else if(isclick){
					multipleOutput.write(CLICKER, key, value);											
				}else if(isshow){					
					multipleOutput.write(SHOWER, key, value);						
				}else if(isvisit){
					multipleOutput.write(VISITOR, key, value);
				}
			}
		}
	}
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: MultipleOutputsFiltering <input path> <output path>");
			return -1;
		}

		Configuration conf = getConf();

		conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

		Job job = Job.getInstance(conf, "MultipleOutputsFiltering");
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		AvroMultipleOutputs.setCountersEnabled(job, true);
		//Specify the Reducer
		job.setNumReduceTasks(0);
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(MultipleOutputFilter.class);
		conf.set("mapreduce.user.classpath.first", "true");

		// Specify the Map
		job.setInputFormatClass(AvroKeyValueInputFormat.class);
		job.setMapperClass(MapClass.class);
		AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setMapOutputValueSchema(job, Session.getClassSchema());

		// Specify input key schema for avro input type.
		AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setInputValueSchema(job, Session.getClassSchema());

		// specify output key schema for avro input type
		AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setOutputValueSchema(job, Session.getClassSchema());

		MultipleOutputs.addNamedOutput(job, "output", TextOutputFormat.class, Text.class, Text.class);

		// output sessions to different files based on category
		AvroMultipleOutputs.addNamedOutput(job, SUBMITTER, AvroKeyValueOutputFormat.class,
				Schema.create(Schema.Type.STRING), Session.getClassSchema());
		AvroMultipleOutputs.addNamedOutput(job, CLICKER, AvroKeyValueOutputFormat.class,
				Schema.create(Schema.Type.STRING), Session.getClassSchema());
		AvroMultipleOutputs.addNamedOutput(job, SHOWER, AvroKeyValueOutputFormat.class,
				Schema.create(Schema.Type.STRING), Session.getClassSchema());
		AvroMultipleOutputs.addNamedOutput(job, VISITOR, AvroKeyValueOutputFormat.class,
				Schema.create(Schema.Type.STRING), Session.getClassSchema());
		

		job.setOutputFormatClass(AvroKeyValueOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(appArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

		// Initiate the map-reduce job, and wait for completion.
		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new MultipleOutputFilter(), args);
		System.exit(res);
	}
}
