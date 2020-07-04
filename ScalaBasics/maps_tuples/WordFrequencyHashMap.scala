package com.tekcrux.maps_tuples

import scala.io.Source

object WordFrequencyHashMap extends App {
   
  val filePath = "E:/Spark/Workspace/ScalaExamples/src/com/tekcrux/maps_tuples/pledge.txt"
       
   val file = Source.fromFile(filePath, "UTF-8")
   val words = file.mkString.split("\\s+")  
   var wordsMap = new scala.collection.immutable.HashMap[String, Int]
  
   words.foreach { token =>
    	wordsMap = wordsMap + (token -> (wordsMap.getOrElse(token, 0) + 1) )
   }
  
   printMap(wordsMap)
  
   def printMap(x: TraversableOnce[_]) {
      println(x.mkString(x.getClass.getSimpleName + " (", ", ", ")"))
   }
}