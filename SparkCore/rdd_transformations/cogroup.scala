package rdd_transformations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object cogroup {
   def main(args: Array[String]) {

      System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
                 
      // Create a Scala Spark Context.
      val conf = new SparkConf().setAppName("cogroup_transformation").setMaster("local")
      val sc = new SparkContext(conf)
      sc.setLogLevel("ERROR")
      
      //Snippet#1: union Transformation
      val rdd1 = sc.parallelize(Seq(
        ("key1", 1),
        ("key2", 2),
        ("key3", 2),
        ("key2", 2),
        ("key1", 3)))
        
      val rdd2 = sc.parallelize(Seq(
        ("key1", 5),
        ("key2", 4),
        ("key4", 4),
        ("key2", 4),
        ("key3", 8) ))
   
      val grouped = rdd1.cogroup(rdd2)
      grouped.foreach(println(_))

   }
}