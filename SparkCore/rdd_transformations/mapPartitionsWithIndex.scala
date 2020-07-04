package rdd_transformations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object mapPartitionsWithIndex extends App {
  
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
   
    // Create a Scala Spark Context.
    val conf = new SparkConf()
                  .setAppName("map_transformation")
                  .setMaster("local")    
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    // ----- mapPartitions -----
    val rdd1 = sc.parallelize(1 to 12, 3)   // (1,2,3,4) (5,6,7,8) (9,10,11,12)    
    val partitioned_map = rdd1.mapPartitionsWithIndex( (index, iter) => List(iter.next + " (" + index + ")").iterator).collect
    
    //val partitioned_map = rdd1.mapPartitions(x => for (i <- x) yield (i*i) ).collect
    println("--- mapPartitionsWithIndex Transformation ---")
    partitioned_map.foreach(println)
         
    // ----- mapPartitionsWithIndex -----
    println("--- mapPartitionsWithIndex Transformation ---")
    val rdd2 =  sc.parallelize(List("white","magenta","red","green","blue","cyan","black"), 3);
    
    val mapped = rdd2.mapPartitionsWithIndex {
                     (index, iterator) => {
                         println("Called in Partition -> " + index)
                         val myList = iterator.toList
                         myList.map(x => x + " - " +  x.length + " -> " + index).iterator
                     }
                 }    
    mapped.foreach(println)
    
    
   
}