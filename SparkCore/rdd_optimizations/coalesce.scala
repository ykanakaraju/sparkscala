package rdd_optimizations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object coalesce extends App {
  
   System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
     
   val conf = new SparkConf().setAppName("map_transformation").setMaster("local")
   val sc = new SparkContext(conf)
    
   val rdd1 = sc.parallelize(1 to 1000, 15)
   println("rdd1.partitions.length = " + rdd1.partitions.length)
   
   // Return a new RDD that is reduced into smaller number of partitions.
   // second param determines shuffling (true/false)
   val rdd2 = rdd1.coalesce(5, false)
   println("rdd2.partitions.length = " + rdd2.partitions.length)

}