package com.refactorlabs.cs378.assign1;

import com.google.common.collect.Lists;
import junit.framework.Assert;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Unit test for the WordCount map-reduce program.
 *
 * @author David Franke (dfranke@cs.utexas.edu)
 */
public class WordCountTest {

	MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;
	ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;

	@Before
	public void setup() {
		WordCount.MapClass mapper = new WordCount.MapClass();
		WordCount.ReduceClass reducer = new WordCount.ReduceClass();

		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	private static final String TEST_WORD = "Yadayada";

	@Test
	public void testMapClass() {
		mapDriver.withInput(new LongWritable(0L), new Text(TEST_WORD));
		mapDriver.withOutput(new Text(TEST_WORD), WordCount.ONE);
		try {
			mapDriver.runTest();
		} catch (IOException ioe) {
			Assert.fail("IOException from mapper: " + ioe.getMessage());
		}
	}

	@Test
	public void testReduceClass() {
		List<LongWritable> valueList = Lists.newArrayList(WordCount.ONE, WordCount.ONE, WordCount.ONE);
		reduceDriver.withInput(new Text(TEST_WORD), valueList);
		reduceDriver.withOutput(new Text(TEST_WORD), new LongWritable(3L));
		try {
			reduceDriver.runTest();
		} catch (IOException ioe) {
			Assert.fail("IOException from mapper: " + ioe.getMessage());
		}
	}
}