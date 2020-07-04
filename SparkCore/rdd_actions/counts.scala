package rdd_actions

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object counts extends App {
  
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
       
    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("sample").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    // count
    val rdd = sc.parallelize(List(1,2,3,4,5,6,5,4,3,2,1))
    val count = rdd.count()
    println(s"count: $count")
    
    //countByValue
    val countByVal = rdd.countByValue()
    println(s"countByValue: $countByVal")
    
    // countByKey
    val arrData = Array(("USA", 1), ("USA", 2), ("India", 1),
                    ("UK", 1), ("India", 4), ("India", 9),
                    ("USA", 8), ("USA", 3), ("India", 4),
                    ("UK", 6), ("UK", 9), ("UK", 5))
                    
    val rdd1 = sc.parallelize(arrData, 3)
    val countByKey = rdd1.countByKey()
    println(s"countByKey: $countByKey")
    
    
    //countApprox
    val timeoutMs = 40
    val confidence = 0.95    
    val countApprox = rdd.countApprox(timeoutMs, confidence)
    println(s"countApprox: $countApprox")
  
}