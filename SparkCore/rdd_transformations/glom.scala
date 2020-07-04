package rdd_transformations

// glom operation allows you to treat a partition 
// as an array rather as single row at time

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object glom extends App {
    
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("partitionBy").setMaster("local")    
    val sc = new SparkContext(conf)
    
    sc.setLogLevel("ERROR")
    
    val dataList = List(10,4,59,6,20,54,23,4,1,56,39,23,12,34,1,2,4,3,21,32,33)
       
    val dataRDD = sc.makeRDD(dataList, 3)  
    
    val numParts = dataRDD.getNumPartitions   
    println(s"Partitions : $numParts")
    
    val maxValue =  dataRDD.reduce (_ max _)
    
    println(s"maxValue: $maxValue")
    
    // A better way is to compute max in each partition and then
    // compute the max from these partition max values. 
    println
    println("Contents of each partition")
    dataRDD.glom().collect().foreach(x => println(x.mkString(",")))
    
    println
    val maxInEachPartition = dataRDD.glom().map(value => value.max)
    maxInEachPartition.collect().foreach(x => println("Max in partition: " + x))
    
    val maxValueGlom = maxInEachPartition.reduce(_ max _)
    println                  
    println(s"maxValueGlom: $maxValueGlom")                  
 
}