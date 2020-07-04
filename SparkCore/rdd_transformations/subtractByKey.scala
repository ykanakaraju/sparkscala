package rdd_transformations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object subtractByKey extends App {
  
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
     
    val conf = new SparkConf().setAppName("union_transformation").setMaster("local")
    val sc = new SparkContext(conf)
    
    val rdd1 = sc.parallelize(List((1, 2), (3, 4), (3, 6), (4, 4), (4, 6), (5, 6)))
    val rdd2 = sc.parallelize(List((3, 4), (4, 2)))
    
    val sbkRdd = rdd1.subtractByKey(rdd2)
    println(sbkRdd.foreach(println(_)))
}