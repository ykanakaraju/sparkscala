package sparkcore

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
//import java.io.File
//import org.apache.commons.io.FileUtils

object WordCount  {
  
  def main(args: Array[String]) {
      System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
         
      val inputFile = args(0)
      val outputFile = args(1)
      
      val conf = new SparkConf().setAppName("wordCount").setMaster("local")
      
      // Create a Scala Spark Context.
      val sc = new SparkContext(conf)
      sc.setLogLevel("ERROR")
      
      // Load our input data.
      val input =  sc.textFile(inputFile)
      
      // Split up into words.
      val words = input.flatMap(line => line.split(" "))
      
      // Transform into word and count.
      val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
      
      // Save the word count back out to a text file, causing evaluation.      
      //FileUtils.deleteQuietly(new File(outputFile));
      
      counts.saveAsTextFile(outputFile)
      //counts.saveAsSequenceFile(outputFile)
  }
}