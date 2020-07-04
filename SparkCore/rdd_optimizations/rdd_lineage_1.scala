package rdd_optimizations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object rdd_lineage_1 extends App {
  
  System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
     
   val conf = new SparkConf().setAppName("rdd_lineage").setMaster("local")
   val sc = new SparkContext(conf)
    
   val rdd1 = sc.textFile("data/serverlog-sample-1.txt")
   
   val rdd2 = rdd1.map(line => line.split(" "))
                  .filter(words => words.length > 0)
                  
   val rdd3 = rdd2.map(words => (words(0),1))
                  .reduceByKey((x, y) => x + y)
   
   println("<-------- 1 -------->")
   val rdd1_lineage = rdd1.toDebugString
   println(rdd1_lineage)
   
   println("<-------- 2 -------->")
   val rdd2_lineage = rdd2.toDebugString
   println(rdd2_lineage)
   
   println("<-------- 3 -------->")
   val rdd3_lineage = rdd3.toDebugString
   println(rdd3_lineage)
}