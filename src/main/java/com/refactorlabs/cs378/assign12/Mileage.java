package com.refactorlabs.cs378.assign12;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;



public class Mileage {

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
        
		Dataset<Row> info2 = sparks.sql("SELECT DISTINCT _c3, _c5, _c9 FROM table1 WHERE CAST(_c9 as INT) != 0");
		info2.repartition(1).write().format("csv").save(outputFilename+"/result2");
	    Dataset<Row> info3 = sparks.read().csv(outputFilename+"/result2");
	    info3.registerTempTable("table2");
	    info3 = sparks.sql("SELECT _c1, MIN(CAST(_c2 AS INT)), MAX(CAST(_c2 AS INT)), AVG(CAST(_c2 AS INT)) FROM table2 GROUP BY _c1 ORDER BY _c1");
	    info3.repartition(1).write().format("csv").save(outputFilename+"/result3");
		sc.stop();
	}
}
