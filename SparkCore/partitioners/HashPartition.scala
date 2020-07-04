package partitioners

import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object HashPartition extends App {
  
   System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("partitionBy").setMaster("local")    
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR") 
      
    val rdd = sc.parallelize(Array(("USA", 1), ("USA", 2), ("India", 1),
                               ("UK", 1), ("India", 4), ("India", 9),
                               ("USA", 8), ("USA", 3), ("India", 4),
                               ("India", 8), ("USA", 3), ("UK", 4),
                               ("USA", 8), ("UK", 3), ("India", 4),
                               ("UK", 6), ("UK", 9), ("UK", 5)), 3)                         
      
    //rdd.mapPartitionsWithIndex( (i, iter) => Iterator((i, iter.toMap))).collect.foreach(println)
    rdd.glom.collect.foreach(x => println(x.mkString(",")) )
    
    val hashPartitioner = new HashPartitioner(3)
    
    val partitionedRdd = rdd.partitionBy(hashPartitioner).persist()
    
    partitionedRdd.foreachPartition( iter => 
          {
             print("Partition: ")
             iter.foreach(x => print(x.toString()))
             println()
          })          
    
}