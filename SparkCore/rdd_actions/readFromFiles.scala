package rdd_actions

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable

object readFromFiles extends App {
  
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
     
    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("sample").setMaster("local")
    val sc = new SparkContext(conf)
    
    val rdd1 = sc.sequenceFile("output_seq/part-00000", classOf[Text], classOf[IntWritable])
    rdd1.foreach(println)
    
    val rdd2 = sc.objectFile("output_obj/part-00000")
    rdd2.foreach(println)    
}