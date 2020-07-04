package rdd_transformations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object sample extends App {
  
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
     
    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("sample").setMaster("local")
    val sc = new SparkContext(conf)
    
    //Snippet#1: union Transformation
    val rdd = sc.parallelize(List(1,2,3,4,5,6,7,8,9), 1)
    
    val sample = rdd.sample(false, 0.5, 11)
    println("1 >> " + sample.collect.mkString(", "))
    
    val sampleReplace = rdd.sample(true, 0.5, 11)
    println("2 >> " + sampleReplace.collect.mkString(", "))
    
}