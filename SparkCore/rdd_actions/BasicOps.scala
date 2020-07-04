package rdd_actions

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object BasicOps {
   def main(args: Array[String]) {
      
      System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
     
      val inputfile = args(0)
       
      // Create a Scala Spark Context.
      val conf = new SparkConf().setAppName("BasicOps").setMaster("local")    
      val sc = new SparkContext(conf)
      
      sc.setLogLevel("ERROR")
      
      // read input testfile
      val babyNames = sc.textFile(inputfile)
  
      //cache this RDD as we are going to use this repeatedly
      babyNames.cache()     
      
      //#snippet1: Count the elements in the RDD
      babyNames.count
      println("Count: " + babyNames.count)
      
      //snippet#2 - first object - first
      val first = babyNames.first
      print("First element in the RDD :" + first)
      
      //snippet#3 - read top 5 records - take & foreach
      babyNames.collect().take(5).foreach(println)
      
      //#snippet4 : distinct & count
      val rows = babyNames.map(line => line.split(","))
      val cities = rows.map(row => row(2))
      val distinctCities = cities.distinct.count      
      println("distinctCities count:" + distinctCities)
      
      //snippet#5 - countByKey & countByValue    
      cities.map(k => (k,1)).countByKey.take(5).foreach(println)
      println("--- countByValue ---")
      cities.map(k => (k,1)).countByValue.take(5).foreach(println)
      
      //#snippet6: filter & contains
      val davidRows = rows.filter(row => row(1).contains("DAVID"))
      println("Count of babies named DAVID: " + davidRows.count)
      
      //snippet#7: DAVID with Age > 10
      val davidRowsAge10plus = davidRows.filter(row => row(4).toInt > 10).count
      println("Count of babies with name DAVID and Age > 10: " + davidRowsAge10plus)
      
       //snippet#8: reduceByKey & sortBy transformations
      val names = rows.map(name => (name(1), 1))
      names.reduceByKey((a,b) => a + b).sortBy(_._2, ascending = false).take(10).foreach ( println )
      
      val filteredRows = babyNames.map(line => line.split(","))
      filteredRows.map ( n => (n(1), n(4).toInt)).reduceByKey((a,b) => a + b).sortBy(_._2, ascending = false).take(5).foreach (println)
           
   }
}