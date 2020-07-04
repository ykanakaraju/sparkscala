package rdd_actions

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.WildcardFileFilter

object saveAs {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
         
      val inputFile = "data/wordcount.txt"
            
      val conf = new SparkConf().setAppName("wordCount").setMaster("local")
      // Create a Scala Spark Context.
      val sc = new SparkContext(conf)
      // Load our input data.
      val input =  sc.textFile(inputFile)
      // Split up into words.
      val words = input.flatMap(line => line.split(" "))
      // Transform into word and count.
      val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
      // Save the word count back out to a text file, causing evaluation.
      
      FileUtils.deleteDirectory(new File("output_txt"))
      counts.saveAsTextFile("output_txt")
      
      FileUtils.deleteDirectory(new File("output_seq"))
      counts.saveAsSequenceFile("output_seq")
      
      FileUtils.deleteDirectory(new File("output_obj"))
      counts.saveAsObjectFile("output_obj")
    }
  
}