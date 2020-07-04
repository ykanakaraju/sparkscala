package com.tekcrux.higherorderfunctions

object map extends App {
  
    (1 to 6).map(x => "*" * x).foreach(println)
    
    println
    
    List(1,2,3,4).map(x => x * x).foreach(println)
    
    println
    
    List("A", "B", "C").map( x => List(x,x) ).foreach(println)
    
    
}