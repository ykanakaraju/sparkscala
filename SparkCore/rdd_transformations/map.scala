package rdd_transformations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object map {
  def main(args: Array[String]) {
    
     System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
          
    //val outputfile = args(0)
    val conf = new SparkConf().setAppName("map_transformation").setMaster("local")
    
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")
    
    //Snippet#1 : map Vs. Flatmap
    val rdd1 = sc.parallelize(List(1, 2, 3))
    val flatmap_output = rdd1.flatMap(x => List(x, x, x)).collect
    val map_output = rdd1.map(x => List(x, x, x)).collect
    
    println("--- Map Transformation ---")
    map_output.foreach(print)
    
    println("--- Flatmap Transformation ---")
    flatmap_output.foreach(print)
    
    //snippet#2 : mapPartitions
    val parallel = sc.parallelize(1 to 12, 3)   // (1,2,3,4) (5,6,7,8) (9,10,11,12)
    val partitioned_map = parallel.mapPartitions(x => List(x.next).iterator).collect
    println("--- mapPartitions Transformation ---")
    partitioned_map.foreach(println)
        
    val parallel1 = sc.parallelize(1 to 9)
    val map_nopartition = parallel1.mapPartitions(x => List(x.next).iterator).collect
    println("--- map without partition ---")
    map_nopartition.foreach(println)
    
    //snippet#3 mapPartitionWithIndex
    val parallel2 = sc.parallelize(1 to 9, 2)   // (1,2,3,4,5) (6,7,8,9)
    val map_withindex=parallel2.mapPartitionsWithIndex( (index: Int, it: Iterator[Int]) => it.toList.map(x => index + ", " + x).iterator).collect
    println("--- Map partitions with index --- ")
    map_withindex.foreach(println)
    
    
  }
}