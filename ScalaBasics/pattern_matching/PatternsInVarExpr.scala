package com.tekcrux.pattern_matching

// Using Patterns in Variables and Expressions

object PatternsInVarExpr extends App {
    val (x, y) = (1, 2)
    println(f"x = $x, y= $y")
    
    val (q, r) = BigInt(10) /% 3
    println(f"q = $q, r= $r")
    
    val arr = Array(1, 7, 2, 9, 1)
    
    val Array(first, second, _*) = arr    
    println(f"first = $first, second= $second")
    
    val Array(f, s, rest @ _*) = arr      // need to use @ to assign rest of te elements to a Seq variable
    println(rest)
    
    println("1-----------------------------------------------")
    
    import scala.collection.JavaConverters._
    // Converts Java Properties to a Scala map—just to get an interesting example
    for ((k, v) <- System.getProperties.asScala) println(k + " -> " + v)
    
    println("2-----------------------------------------------")
    for ((k, "") <- System.getProperties.asScala) println(k) 
    
    println("3-----------------------------------------------")    
    for ((k, v) <- System.getProperties.asScala if v == "") println(k)
  
}