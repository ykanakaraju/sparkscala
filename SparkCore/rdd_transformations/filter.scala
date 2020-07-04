package rdd_transformations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object filter extends App {
  
    //System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
   
    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("map_transformation").setMaster("local")    
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    // ----- mapPartitions -----
    val rdd1 = sc.parallelize(1 to 20)
    val rdd2 = rdd1.filter(_ % 2 == 0)
    //rdd2.collect.foreach(println)
    
    print(rdd2.collect.mkString(","))
}