package rdd_transformations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object mapPartitions extends App {
  
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("mapPartitions").setMaster("local")    
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    // ----- mapPartitions -----
    val rdd1 = sc.parallelize(1 to 12, 3)   // (1,2,3,4) (5,6,7,8) (9,10,11,12)    
    
    //val partitioned_map = rdd1.mapPartitions(x => List(x.next).iterator).collect
    val partitioned_map = rdd1.mapPartitions(x => List(x.next).iterator ).collect           
                 
    println("--- partitioned_map ---")
    partitioned_map.foreach(println) 
    
    val partitioned_map_2 = rdd1.mapPartitions(x => { x.map(a => a*a) } ).collect   
    println("--- partitioned_map_2 ---")     
    partitioned_map_2.foreach(println) 
    
    
    val partitioned_map_3 = rdd1.mapPartitions(x => {val y = x.reduce((a,b)=>a+b); List(y).iterator} ).collect
    println("--- partitioned_map_3 ---")
    partitioned_map_3.foreach(println) 
    
    
    
    
}






