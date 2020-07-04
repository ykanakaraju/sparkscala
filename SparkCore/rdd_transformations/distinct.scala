package rdd_transformations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object distinct {
  def main(args: Array[String]) {
    
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
     
    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("distinct_transformation").setMaster("local")
    val sc = new SparkContext(conf)
    
    //Snippet#1: union Transformation
    val parallel = sc.parallelize(1 to 9)
    val par2 = sc.parallelize(5 to 15)
    parallel.union(par2).distinct.collect.foreach(println)
    
    //println("distinct :" + distinct.foreach(println(_))) 
  }
}