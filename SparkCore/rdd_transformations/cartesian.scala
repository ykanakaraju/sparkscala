package rdd_transformations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object cartesian {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
        
    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("cartesian_transformation").setMaster("local")
    val sc = new SparkContext(conf)
    
    //Snippet#1: union Transformation
    val x = sc.parallelize(List(1, 2, 3, 4))
    val y = sc.parallelize(List(6, 7, 8, 9))
    x.cartesian(y).collect.foreach(println(_))
  }
}