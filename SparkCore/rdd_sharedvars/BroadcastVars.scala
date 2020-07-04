package rdd_sharedvars

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

object BroadcastVars extends App {
  
  System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
  // Create a Scala Spark Context.
  val conf = new SparkConf().setAppName("map_transformation").setMaster("local")    
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")
  
  val lookup: Broadcast[Map[Int, String]] = sc.broadcast(Map(1 -> "a", 2 -> "e", 3 -> "i", 4 -> "o", 5 -> "u")) 

  val result = sc.parallelize(Array(2, 1, 3)).map(lookup.value(_)) 
  println(result.getClass)
  result.collect.foreach(println)
  
  
  //Snippet 2
  println("=========== 2 ===========")
  val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")
  val words = sc.parallelize(myCollection, 2)
  val lookupData = Map("Spark" -> 1000, "Definitive" -> 200, "Big" -> -300, "Simple" -> 100)
  val lookupBroadcast = sc.broadcast(lookupData)
  words.map(word => (word, lookupBroadcast.value.getOrElse(word, 0)))
       .sortBy(wordPair => wordPair._2)
       .collect()
       .foreach(println)
  
  //Snippet 3
  println("=========== 3 ===========")
  val hoods = Seq((1, "Mission"), (2, "SOMA"), (3, "Sunset"), (4, "Haight Ashbury"))
  val checkins = Seq((234, 1),(567, 2), (234, 3), (532, 2), (234, 4))
  val hoodsRdd = sc.parallelize(hoods)
  val checkRdd = sc.parallelize(checkins)

  val broadcastedHoods = sc.broadcast(hoodsRdd.collectAsMap())
  
  val checkinsWithHoods = checkRdd.mapPartitions(
         {row => row.map(x => (x._1, x._2, broadcastedHoods.value.getOrElse(x._2, -1))) }
      )
         
   checkinsWithHoods.take(5).foreach(println)
       
}

