package com.tekcrux.java.sparkstreaming;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class JavaHDFSWordCount {

	public static void main(String[] args) throws Exception {
		
		if (args.length < 1) {
	      System.err.println("Usage: JavaHDFSWordCount <directory>");
	      System.exit(1);
	    }
		
		// Create the SparkConfig
		SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount").setMaster("local[2]");
		
		// Create the context with a 1 second batch size
	    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
	    
	    JavaDStream<String> lines = ssc.textFileStream(args[0]);
	    JavaDStream<String> words = lines.flatMap( x -> Arrays.asList(x.split(" ")));	 
	    words.foreachRDD( x-> { x.collect().stream().forEach(n-> System.out.println("word: " + n)); });
	    
	    // Aggregate the word into pairs and compute the word frequency
	    JavaPairDStream<String, Integer> wordsPairs = words.mapToPair((s) -> new Tuple2<String, Integer>(s, 1));    
	    JavaPairDStream<String, Integer> wordCounts = wordsPairs.reduceByKey((a,b) -> a+b);
	    	
	    wordCounts.print();
	    
	    ssc.start();
	    ssc.awaitTermination();
	}
}
