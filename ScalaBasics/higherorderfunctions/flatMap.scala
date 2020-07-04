package com.tekcrux.higherorderfunctions

object flatMap extends App {
  
  val l = List("Ant", "Ball", "Cat").flatMap( x => List(x,x) )
  l.foreach(println)
  
  val l2 = List("A", "B", "C").map( x => List(x,x) )
  l2.foreach(println)
}