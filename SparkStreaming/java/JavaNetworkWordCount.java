package com.tekcrux.java.sparkstreaming;

import java.util.Arrays;

import scala.Tuple2;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.tekcrux.sparkstreaming.StreamingExamples;

public class JavaNetworkWordCount {	

	public static void main(String[] args) throws Exception {
		
		// To run this on your local machine, you need to first run a Netcat server
		// $ nc -lk 9999
		 
	    if (args.length < 2) {
	      System.err.println("Usage: JavaNetworkWordCount <hostname> <port>");
	      System.exit(1);
	    }
	
	    StreamingExamples.setStreamingLogLevels();	    
	    //Logger.getRootLogger().setLevel(Level.ERROR);	
	
	    // Create the SparkConfig
	    SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount").setMaster("local[2]");
	    
	    // Create the context with a 1 second batch size
	    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
	    
	    // Read the data from the StreamingContext into a DStream
	    JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
	            args[0], Integer.parseInt(args[1]), StorageLevels.MEMORY_AND_DISK_SER);	    
	    
	    // Split up the data read into words
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
