package com.tekcrux.higherorderfunctions

object transform extends App {
  
  val names = scala.collection.mutable.ArrayBuffer("Kanakaraju", "Veer")
  
  names.foreach(println)  
  
  names.transform( x => x.length().toString() )
       .foreach(println)
       
  names.foreach(println)     
}