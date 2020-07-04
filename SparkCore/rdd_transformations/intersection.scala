package rdd_transformations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object intersection {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
     
    //val outputfile = args(0)
    
    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("intersection_transformation").setMaster("local")
    val sc = new SparkContext(conf)
    
    //Snippet#1: intersection Transformation
    val parallel = sc.parallelize(1 to 9)
    val par2 = sc.parallelize(5 to 15)
    val intersect = parallel.intersection(par2).collect.foreach(println(_))
    println("intersection :" + intersect)

  
  }
}