package rdd_actions

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object takeSample extends App {
  
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
     
    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("sample").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val rdd = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10,11,12))
    
    // replacement = false. i.e the sample is not replaced in the population.
    val sample1 = rdd.takeSample(false, 6)
    println("sample1 >> " + sample1.mkString(", "))
    
    // replacement = true. i.e the sample is put back in the population for the next sample
     val sample2 = rdd.takeSample(true, 6)
    println("sample2 >> " + sample2.mkString(", "))
    
    
     /*     
     When we sample with replacement, the two sample values are independent. 
     This means that what we get on the first one doesn't affect 
     what we get on the second. 

     In sampling without replacement, the two sample values aren't independent. 
     This means that what we got on the for the first one affects 
     what we can get for the second one.       
     **/
    

}