package rdd_optimizations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.{HashPartitioner, RangePartitioner}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object partitionBy extends App {
   
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("partitionBy").setMaster("local")    
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val data = Array[(String, Int)](("K", 1), ("T", 5), ("T", 7), ("W", 8), ("W", 5), ("W", 6), 
                                     ("A", 2), ("A", 3), ("B", 2), ("B", 3), ("D", 8), ("D", 7))
                                     
    val pairs = sc.parallelize(data, 3)
    pairs.glom().collect().foreach( x => x.foreach(println)) 
    
    // Some Basic info
    println("rdd.partitioner: " + pairs.partitioner)
    println("rdd.partitions.length: " + pairs.partitions.length)
    
    
    val result = pairs.partitionBy(new RangePartitioner(3, pairs, true))
    //val result = pairs.partitionBy(new HashPartitioner(2))
    result.glom().collect().foreach(println)  
    
    // Some Basic info about the result RDD
    println("result.partitioner: " + result.partitioner)
    println("result.partitions.length: " + result.partitions.length)
    
    // repartition
    /* val rpRdd = pairs.repartition(4)
    println("rpRdd.partitioner: " + rpRdd.partitioner)
    println("rpRdd.partitions.length: " + rpRdd.partitions.length) */
}