package rdd_optimizations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object storage_level extends App {
  
   System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
     
   val conf = new SparkConf().setAppName("map_transformation").setMaster("local")
   val sc = new SparkContext(conf)
   
   val c = sc.parallelize(1 to 100, 5)
   println(c.getStorageLevel.description)
   
   //c.cache()
   //println(c.getStorageLevel.description)
   
   c.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
   println(c.getStorageLevel.description)
   
   
   
   c.count()
   c.unpersist(true)
   
}