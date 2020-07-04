package rdd_actions

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object takeOrdered extends App {
  
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
     
    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("sample").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val rdd = sc.parallelize(Seq(3,9,2,3,5,4,7,8,3,5,6), 2)
    
    val ordered1 = rdd.takeOrdered(5)
    println(ordered1.mkString(", "))
        
    val ordered2 = rdd.takeOrdered(5)(Ordering[Int].reverse)
    println(ordered2.mkString(", "))

}