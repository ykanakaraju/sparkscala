package rdd_actions

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object reduce extends App {
  
  System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
     
    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("sample").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val rdd = sc.parallelize(List(1,2,3,4,5,6))
    
    val plus = rdd.reduce((x,y) => x + y)
    println("plus = " + plus)   
    
    val minus = rdd.reduce((x,y) => x - y)
    println("minus = " + minus)  
        
    // will averaging work ?? 
    // its imporatant that the function we apply need to be commutative and associative
    val avg : Double = rdd.reduce((x,y) => (x + y)/2 )
    println("avg = " + avg) 
}