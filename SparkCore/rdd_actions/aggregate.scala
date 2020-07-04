package rdd_actions

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object aggregate {
   def main(args: Array[String]) {  
      System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
      
      val conf = new SparkConf().setAppName("aggregate by key").setMaster("local")
      val sc = new SparkContext(conf)
          
      val list = List(1,2,3,4,5,6,7,8,9,10)   // => (55, 10)     
       val rdd = sc.parallelize(list)
       
      // aggregate method takes three parameters 
      // 1. a zero value initializer. Here it is (0,0) as we are expecing a pair as output
      // 2. a sequence function with an accumulator that works per partition and applies 
      // to each element in the partition 
      // 3. the third combiner function merges all the accumulators across partitions
      
      val sumCount = list.aggregate(0, 0)((x, y) => (x._1 + y, x._2 + 1), (a,b) => (a._1 + b._1, a._2 + b._2))
      //val sumCount = rdd.aggregate(0, 0)((x, y) => (x._1 + y, x._2 + 1), (a,b) => (a._1 + b._1, a._2 + b._2))
      println("Avg = " + sumCount._1.toDouble/sumCount._2)           
      
   }
}