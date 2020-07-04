package rdd_sharedvars

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

object BroadcastTest1 extends App {
      
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
    // Create a Scala Spark Context.
    val blockSize = "4096"
    val conf = new SparkConf().setAppName("map_transformation")
                              .set("spark.broadcast.blockSize", blockSize)
                              .setMaster("local")    
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val slices = 2
    val num = 100000

    val arr1 = (0 until num).toArray

    for (i <- 0 until 3) {
      println(s"Iteration $i")
      println("===========")
      val startTime = System.nanoTime
      
      val barr1 = sc.broadcast(arr1)
      
      val observedSizes = sc.parallelize(1 to 10, slices)
                            .map(_ => barr1.value.length)
      
      // Collect the small RDD so we can print the observed sizes locally.
      observedSizes.collect().foreach(i => println(i))
      
      println("Iteration %d took %.0f milliseconds".format(i, (System.nanoTime - startTime) / 1E6))
    }

    
}