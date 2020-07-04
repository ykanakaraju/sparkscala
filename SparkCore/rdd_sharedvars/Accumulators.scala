package rdd_sharedvars

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.Accumulator

object Accumulators extends App {
  
   System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
   // Create a Scala Spark Context.
   val conf = new SparkConf().setAppName("map_transformation").setMaster("local")    
   val sc = new SparkContext(conf)
  
   val count = sc.longAccumulator("counter")    
   
   val result = sc.parallelize(List(1,2,3,4,5,6), 2)
                 .map(i => { count.add(1); i })  
                 .reduce((x, y) => x + y)
                 
   //assert(count.value == 3) 
   //assert(result == 6) 
   
   println("count.value : " + count.value)
   println("result : " + result)

}