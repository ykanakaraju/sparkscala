package rdd_transformations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import scala.collection.mutable

object aggregateByKey extends App {
  
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
    val conf = new SparkConf().setAppName("aggregate by key").setMaster("local")
    val sc = new SparkContext(conf)   
          
    sc.setLogLevel("ERROR")
    
    val keysWithValuesList = Array("foo=A", "foo=A", "foo=A", "foo=B", "foo=B", "bar=C", "bar=D", "bar=D")

    val data = sc.parallelize(keysWithValuesList)
     
    //Create key value pairs
    val kv = data.map(_.split("=")).map(v => (v(0), v(1))).cache()

    kv.foreach(println) 
       
    val initialCount = 0;
    val addToCounts = (n: Int, v: String) => n + 1
    val sumPartitionCounts = (p1: Int, p2: Int) => p1 + p2

    val countByKey = kv.aggregateByKey(initialCount)(addToCounts, sumPartitionCounts)       

    println("------------------")

    println("Aggregate By Key sum Results")
    val sumResults = countByKey.collect()
    sumResults.foreach(println)
    
    /*for(indx <- sumResults.indices){
        val r = sumResults(indx)
        println(r._1 + " -> " + r._2)
    }*/
    
    
    val initialSet = mutable.HashSet.empty[String]
    val addToSet = (s: mutable.HashSet[String], v: String) => s += v    
    val mergePartitionSets = (p1: mutable.HashSet[String], p2: mutable.HashSet[String]) => p1 ++= p2

    val uniqueByKey = kv.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
    
    println("Aggregate By Key unique Results")

    val uniqueResults = uniqueByKey.collect()
    uniqueResults.foreach(println)
    
    /*for(indx <- uniqueResults.indices){
        val r = uniqueResults(indx)
        println(r._1 + " -> " + r._2.mkString(","))
    }*/  

}