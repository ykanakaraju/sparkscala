package com.tekcrux.higherorderfunctions

object reduceLeft extends App {
  
    val a = (1 to 10).reduceLeft(_ - _)
    println(s"a = $a")
    
    val b = List(1,2,3,4,5,6).reduceLeft((a, b) => a * b)
    println(s"b = $b")
    
    val c = List("Adam", "Bob", "Charlie", "David").reduceLeft( (a, b) => if (a.length > b.length) a else b )
    println(s"c = $c")
}