package rdd_transformations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object mapPratitions2 extends App {
  
   System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

   val conf = new SparkConf().setAppName("mapPratitions2").setMaster("local")    
   val sc = new SparkContext(conf)
    
   def combine(c1: (Int, Int), c2: (Int, Int)) : (Int, Int) = {
      (c1._1 + c2._1, c1._2 + c2._2)
   }
      
   def partitionCombine(iter: Iterator[Int]) : Iterator[Tuple2[Int, Int]] = {
      var (sum, count) = (0, 0)      
      for (i <- iter){
        sum += i; count += 1
      }
      List((sum, count)).iterator
   }
   
   val list = List(1,2,3,4,5,6,7,8,9,10)
   val rdd = sc.parallelize(list.toSeq, 3)
   
   // SumCount using mapPartitions
   rdd.mapPartitions( partitionCombine ).foreach(println)
   val partitionSumCount = rdd.mapPartitions( partitionCombine ).reduce( combine )
   println("partitionSumCount: " + partitionSumCount)
      
   // SumCount using mapPartitionsWithIndex
   val numPartitions = rdd.getNumPartitions   
   rdd.mapPartitionsWithIndex((index: Int, it: Iterator[Int]) => it.toList.map(x => index + ", " + x).iterator).foreach(println)
   val pwiSumCount = rdd.mapPartitionsWithIndex((index: Int, it: Iterator[Int]) => partitionCombine(it)).reduce( combine )
   println("pwiSumCount: " + pwiSumCount)  
      
   // SumCount baic
   val basicSumCount = rdd.map(num => (num,1)).reduce(combine)
   println("basicSumCount: " + basicSumCount)
}

 /*def basicSumCount(nums: List[Int]) : (Int, Int) = {
    nums.map(num => (num,1)).reduce(combine)
 }*/