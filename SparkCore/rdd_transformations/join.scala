package rdd_transformations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object join {
   
   def main(args: Array[String]) {
     
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
              
        // Create a Scala Spark Context.
        val conf = new SparkConf().setAppName("join_transformation").setMaster("local")
        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        
        //Snippet#1: join Transformation
        val names1 = sc.parallelize(List("harsha", "aditya", "raju", "veer", "raju")).map(a => (a, 1))
        val names2 = sc.parallelize(List("amrita", "raju", "veer", "anita")).map(a => (a, 1))
               
        names1.foreach(println)
        println("---------")
        names2.foreach(println)
        println("---------\n\n")
       
        
        val join = names1.join(names2)     // Inner Join
        
        println("\nJoin: ")
        join.collect.foreach(println(_))        
               
        println("\nLeft Outer Join: ")      
        val leftOuterJoin = names1.leftOuterJoin(names2)
        leftOuterJoin.collect.foreach(println(_))
        // println("Left Outer Join"+ leftOuterJoin.foreach(println(_)))
       
        println("\nRight outer Join: ")
        val rightOuterJoin = names1.rightOuterJoin(names2)
        rightOuterJoin.collect.foreach(println(_)) 
        
        println("\nFull outer Join: ")
        val fullOuterJoin = names1.fullOuterJoin(names2)
        fullOuterJoin.collect.foreach(println(_)) 
        
        
    }
}