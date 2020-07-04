package rdd_transformations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object sortBy {
  def main(args: Array[String]) {
    
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
     
    val inputfile = "data/wordcount.txt";  
    val conf = new SparkConf().setAppName("sortBy").setMaster("local")
    
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val data = sc.textFile(inputfile)

    val words = data.flatMap( x => x.split(" ") )
    
    val sortedWords = words.sortBy( x => x.substring(x.length - 1) )    
   
    sortedWords.foreach(println)
  }
}