package rdd_transformations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object groupBy extends App {
  
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
  
    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("partitionBy").setMaster("local")    
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    try {
        val dataRdd = sc.parallelize(  Array("Joseph", "Jimmy", "Tina", "Thomas", "James", "Cory",
	                        "Christine", "Jackeline", "Juan"), 2);
      
        val grpRdd = dataRdd.groupBy(word => word.charAt(0));        
        println(" ------ groupBy 1 ------");        
        grpRdd.foreach( x => println(x._1 + " -> " + x._2) )
        
        
        //snippet 2
        println(" ------ groupBy 2 ------");  
        val dataRdd2 = sc.parallelize(Array(1,2,3,4,5,6,7,8,9,10,11,12,13), 1)
        val grpRdd2 = dataRdd2.groupBy(i => i%3)
        println(grpRdd2.collect.mkString("[", ",", "]"))

    }
    catch {
       case e: ArrayIndexOutOfBoundsException => println(e.getMessage)
		}
} 