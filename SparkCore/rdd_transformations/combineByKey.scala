package rdd_transformations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object combineByKey extends App {
  
   System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
   val conf = new SparkConf().setAppName("combineByKey").setMaster("local")
   val sc = new SparkContext(conf)   
   
   sc.setLogLevel("ERROR")
    
   //type PersonScores = (String, (Int, Double))

   val initialScores = Array(("Amrita", 88.0), ("Amrita", 95.0), ("Amrita", 91.0), 
                             ("Aditya", 93.0), ("Aditya", 95.0), ("Aditya", 88.0))

   val initialScoresCached = sc.parallelize(initialScores).cache()

   val createScoreCombiner = (score: Double) => (1, score)     //(Int, Double) (Count, Sum) (1, 88)

   val scoreCombiner = (collector: (Int, Double), score: Double) => {
           (collector._1 + 1, collector._2 + score)
   }

   val scoreMerger = (collector1: (Int, Double), collector2: (Int, Double)) => {
          (collector1._1 + collector2._1, collector1._2 + collector2._2)
   }
   
   val scores = initialScoresCached.combineByKey(createScoreCombiner, scoreCombiner, scoreMerger)

   scores.foreach( println )
   
   
   val averagingFunction = (personScore: (String, (Int, Double))) => {
         (personScore._1, personScore._2._2 / personScore._2._1)
   }

   val averageScores = scores.collectAsMap().map(averagingFunction)

   println("Average Scores using CombingByKey")
      averageScores.foreach((ps) => {
         println(ps._1 + "'s average score : " + ps._2)
      })
    
}