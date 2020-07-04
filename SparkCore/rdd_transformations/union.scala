package rdd_transformations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object union {
  def main(args: Array[String]) {
    
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
     
    //val outputfile = args(0)
    val conf = new SparkConf().setAppName("union_transformation").setMaster("local")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    //Snippet#1: union Transformation
    
    val rdd1 = sc.parallelize(1 to 9)
    val rdd2 = sc.parallelize(5 to 15)
    val union = rdd1.union(rdd2).collect
    println(union.foreach(println(_))) 
  }
}