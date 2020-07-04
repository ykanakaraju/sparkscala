package rdd_transformations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object flatMap extends App {
  
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("partitionBy").setMaster("local")    
    val sc = new SparkContext(conf)
    
    val input =  sc.textFile("data/wordcount.txt")
    
    // 1 - Split up into words.
    val words = input.flatMap(_.split(" "))
    //words.foreach(println)
    println(words.collect.mkString("[", ",", "]"))
    
    //2
    val data = sc.parallelize(List("spark",  "scala", "flatmap"))
    val letters = data.flatMap(x => for(a <- x) yield(a))
    println(letters.collect.mkString("[", ",", "]"))
    
    //val mapletters = data.map(x => for(a <- x) yield(a))
    //println(mapletters.collect.mkString("[", ",", "]"))
    
    //3
    val data2 = sc.parallelize(List(Array(1,2,3), Array(4,5,6), Array(7,8,9)))
    val numbers = data2.flatMap(x => for(a <- x) yield(a))
    println(numbers.collect.mkString("[", ",", "]"))
}