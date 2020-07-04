package rdd_transformations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object groupByKey extends App {
  
  System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
  
  // Create a Scala Spark Context.
  val conf = new SparkConf().setAppName("map_transformation").setMaster("local")    
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")
  
  val rdd1 = sc.parallelize(Array(("USA", 1), ("USA", 2), ("India", 1),
                               ("UK", 1), ("India", 4), ("India", 9),
                               ("USA", 8), ("USA", 3), ("India", 4),
                               ("UK", 6), ("UK", 9), ("UK", 5)), 3)
                               
  println("rdd1.getNumPartitions=" + rdd1.getNumPartitions)                                
                               
  // groupByKey with default partitions
  val grpBy = rdd1.groupByKey(2)
  grpBy.collect.foreach(println)
  println("grpBy.getNumPartitions=" + grpBy.getNumPartitions)
  
  // Compute the count and sum
  println("==== count & sum using groupByKey ====")
  val sumByKey = rdd1.groupByKey().map(p => (p._1, (p._2.size, p._2.sum)))
  sumByKey.collect.foreach(println)
  
  // Is there a better way of doing the same??
  println("==== count & sum using reduceByKey ====")
  val betterSumByKey = rdd1.map(p => (p._1, (1, p._2)))
                           .reduceByKey( (v1, v2) => (v1._1 + v2._1, v1._2 + v2._2) )
                        
  betterSumByKey.collect.foreach(println)                      
                     
                        
   
}