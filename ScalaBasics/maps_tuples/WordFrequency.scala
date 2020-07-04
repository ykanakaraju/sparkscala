package com.tekcrux.maps_tuples

import scala.io.Source

object WordFrequency extends App {
  
   val filePath = "pledge.txt"
       
   val file = Source.fromFile(filePath, "UTF-8")
   val words = file.mkString.split("\\s+")  
   val wordsMap = new scala.collection.mutable.HashMap[String, Int]
  
   words.foreach { token =>
    	if (wordsMap.contains(token))	wordsMap(token) += 1
    	else wordsMap(token) = 1
   }
  
   printMap(wordsMap)
  
   def printMap(x: TraversableOnce[_]) {
      println(x.mkString(x.getClass.getSimpleName + " (", ", ", ")"))
   }

}