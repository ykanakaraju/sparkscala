package com.tekcrux.pattern_matching

// **** matching Arrays, Lists and Tuples ****

object PatternMatch3 extends App {
    
    val arr10 = Array(Array(0), Array(1, 0), Array(0, 1, 0), Array(1, 1, 0))
    
    println("1: Matching Arrays")
    for (arr <- Array(Array(0), Array(1, 0), Array(0, 1, 0), Array(1, 1, 0))) {    
        val result = arr match {
          case Array(0) => "0"
          case Array(x, y) => x + " " + y
          case Array(0, _*) => "0 ..."
          case _ => "something else"
        }      
        println(result)
    }
    
    println("\n2: Matching Lists")
    for (lst <- Array(List(0), List(1, 0), List(0, 0, 0), List(1, 0, 0))) {    
        val result = lst match {
          case 0 :: Nil => "0"
          case x :: y :: Nil => x + " " + y
          case 0 :: tail => "0 ..."
          case _ => "something else"
        }      
        println(result)
    }
    
    println("\n3: Matching Pairs/Tuples")
    for (pair <- Array((0, 1), (1, 0), (1, 1), (1,2,3))) {
        val result = pair match {
          case (0, _) => "0 ..."
          case (y, 0) => y + " 0"
          case (x, y, z) => x +  " " + y + " " + z
          case _ => "nothing matched"
        }    
        println(result)
    }

}