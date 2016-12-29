package com.refactorlabs.cs378.assign12;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
//import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;



public class VIN {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String inputFilename = args[0];
		String outputFilename = args[1];

		// Create a Java Spark context
		SparkConf conf = new SparkConf().setAppName(DataSets.class.getName()).setMaster("local");
		SparkContext sc = new SparkContext(conf);
        SparkSession  sparks = new SparkSession(sc);
		Dataset<Row> info = sparks.read().csv(inputFilename);
		info.registerTempTable("table1"); 
		//sparks.createTempView("table1", info); 
		
		Dataset<Row> info2 = sparks.sql( "SELECT _c3, _c1 FROM table1");
        //JavaRDD<Row> corr = info2.javaRDD();
        info2.repartition(1).write().format("csv").save(outputFilename+"/result3");
        
	 // corr.saveAsTextFile(outputFilename);
		// Transform into word and value
		sc.stop();
	}
}
