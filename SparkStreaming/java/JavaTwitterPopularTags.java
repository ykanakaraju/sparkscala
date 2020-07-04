package com.tekcrux.java.sparkstreaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.codec.language.Soundex;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import com.tekcrux.sparkstreaming.StreamingExamples;

import scala.Tuple2;
import twitter4j.Status;

@SuppressWarnings("unused")
public class JavaTwitterPopularTags {

	private static final Pattern SPACE = Pattern.compile(" ");
	
	public static void main(String[] args) {
		if (args.length < 4) {
	      System.out.println("Usage: JavaTwitterPopularTags <consumerKey> <consumerSecret> <accessToken> <accessTokenSecret>");
	      System.exit(1);
	    }
		
		StreamingExamples.setStreamingLogLevels();
		
		System.setProperty("twitter4j.oauth.consumerKey", args[0]);
	    System.setProperty("twitter4j.oauth.consumerSecret", args[1]);
	    System.setProperty("twitter4j.oauth.accessToken", args[2]);
	    System.setProperty("twitter4j.oauth.accessTokenSecret", args[3]);
	    
	    SparkConf sparkConf = new SparkConf().setAppName("JavaTwitterPopularTags").setMaster("local[2]");
	    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
	    JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(ssc);
	    	    
	    //Filter hash Tags      
	    JavaDStream<String> words = stream.flatMap( (status) -> Arrays.asList(status.getText().split(" ")));
	    words.foreachRDD( x-> { x.collect().stream().forEach(n-> System.out.println("item of list: " + n)); });
	    
	    
	    JavaDStream<String> hashTags = words.filter((s) -> s.startsWith("#"));	    
	    hashTags.foreachRDD( x-> { x.collect().stream().forEach(n-> System.out.println("item of list: " + n)); });
	    
	    //most repeated hash tags in last 10 seconds window
	    JavaPairDStream<String, Integer> hashTagsOnes = hashTags.mapToPair((s) -> new Tuple2<String, Integer>(s, 1));	    
	    JavaPairDStream<String, Integer> topCount10 = hashTagsOnes.reduceByKeyAndWindow((a,b) -> a+b, Durations.seconds(10));
	    
	    JavaPairDStream<String, Integer> topCount10pairs 
	    		= topCount10.mapToPair((s) -> s.swap())
							.transformToPair((rdd) -> rdd.sortByKey(false))
							.mapToPair((s) -> s.swap());
	    
	    // Print 10 popular hashtags
	    topCount10pairs.foreachRDD( (rdd) -> {
	    	List<Tuple2<String, Integer>> topList = rdd.take(10);
	    	Map<String, Tuple2<String, Integer>> resultmap = new HashMap<>(10);
			System.out.println("----------------------------------------------------");
			System.out.println(String.format("Popular topics out of %s total topics received:\n", rdd.count()));
			topList.forEach(x -> System.out.println(String.format("%s (%s tweets)", x._1, x._2)));
	    });	    
	    
	    ssc.start();
	    ssc.awaitTermination();
	}
}
