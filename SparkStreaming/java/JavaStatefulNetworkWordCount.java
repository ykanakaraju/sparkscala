package com.tekcrux.java.sparkstreaming;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import scala.Tuple2;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.tekcrux.sparkstreaming.StreamingExamples;

public class JavaStatefulNetworkWordCount {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) {

	// function to compute the word frequency in a stateful manner.
    final Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction =
        new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
          @Override
          public Optional<Integer> call(List<Integer> values, Optional<Integer> state) {
            Integer newSum = state.or(0);
            for (Integer value : values) {
              newSum += value;
            }
            return Optional.of(newSum);
          }
        };

    StreamingExamples.setStreamingLogLevels();	
        
    SparkConf sparkConf = new SparkConf().setAppName("JavaStatefulNetworkWordCount").setMaster("local[2]");
    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
    ssc.checkpoint(".");

    List<Tuple2<String, Integer>> tuples = Arrays.asList(new Tuple2<String, Integer>("hello", 1),
            new Tuple2<String, Integer>("world", 1));
    
    JavaPairRDD<String, Integer> initialRDD = ssc.sparkContext().parallelizePairs(tuples);

    JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
            args[0], Integer.parseInt(args[1]), StorageLevels.MEMORY_AND_DISK_SER_2);
        
    JavaDStream<String> words = lines.flatMap( x -> Lists.newArrayList(SPACE.split(x)));	
    
    JavaPairDStream<String, Integer> wordsDstream = words.mapToPair((s) -> new Tuple2<String, Integer>(s, 1));    
        
    JavaPairDStream<String, Integer> stateDstream = wordsDstream.updateStateByKey(updateFunction,
            new HashPartitioner(ssc.sparkContext().defaultParallelism()), initialRDD);

    stateDstream.print();
    ssc.start();
    ssc.awaitTermination();
  }
}