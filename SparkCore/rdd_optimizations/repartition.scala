package rdd_optimizations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object repartition extends App {
  
  System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("partitionBy").setMaster("local")    
    val sc = new SparkContext(conf)
    
    val data = Array[(String, Int)]( ("K", 1), ("T", 5), ("T", 7), ("W", 8), ("W", 5), ("W", 6), 
                                     ("A", 2), ("A", 3), ("B", 2), ("B", 3), ("D", 8), ("D", 7) )
                                     
    val pairs = sc.parallelize(data, 2)
    //println("pairs.partitioner: " + pairs.partitioner)
    println("pairs.partitions.length: " + pairs.partitions.length)
    
    // repartition
    val rpRdd = pairs.repartition(4)
    //println("rpRdd.partitioner: " + rpRdd.partitioner)
    println("rpRdd.partitions.length: " + rpRdd.partitions.length)
}