package rdd_transformations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object reduceByKey extends App {
  System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
  // Create a Scala Spark Context.
  val conf = new SparkConf().setAppName("map_transformation").setMaster("local")    
  val sc = new SparkContext(conf)
  
  val rdd1 = sc.parallelize(Array(("USA", 1), ("USA", 2), ("India", 1),
                               ("UK", 1), ("India", 4), ("India", 9),
                               ("USA", 8), ("USA", 3), ("India", 4),
                               ("UK", 6), ("UK", 9), ("UK", 5)))
                               
  // groupByKey with default partitions
  val rdd2 = rdd1.reduceByKey((x,y) => x + y)
  
  rdd2.foreach(println)
  
}