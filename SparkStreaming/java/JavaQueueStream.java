package com.tekcrux.java.sparkstreaming;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import scala.Tuple2;

import com.google.common.collect.Lists;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.tekcrux.sparkstreaming.StreamingExamples;

public final class JavaQueueStream {
  private JavaQueueStream() {
  }

  public static void main(String[] args) throws Exception {

    StreamingExamples.setStreamingLogLevels();
    SparkConf sparkConf = new SparkConf().setAppName("JavaQueueStream").setMaster("local[2]");

    // Create the context
    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));

    // Create the queue through which RDDs can be pushed to
    // a QueueInputDStream
    Queue<JavaRDD<Integer>> rddQueue = new LinkedList<>();

    // Create and push some RDDs into the queue
    List<Integer> list = Lists.newArrayList();
    for (int i = 0; i < 1000; i++) {
      list.add(i);
    }

    for (int i = 0; i < 30; i++) {
      rddQueue.add(ssc.sparkContext().parallelize(list));
    }

    // Create the QueueInputDStream and use it do some processing
    JavaDStream<Integer> inputStream = ssc.queueStream(rddQueue);
        
    JavaPairDStream<Integer, Integer> mappedStream = inputStream.mapToPair((i) -> new Tuple2<Integer, Integer>(i % 10, 1));	    
    
    JavaPairDStream<Integer, Integer> reducedStream = mappedStream.reduceByKey((a,b) -> a+b);
    
    reducedStream.print();
    ssc.start();
    ssc.awaitTermination();
  }
}
