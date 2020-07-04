package com.tekcrux.pattern_matching

object PatternMatch2 extends App {
  
    val arr = Array(1, 1.5, "test")
    print(arr)
    
    for (obj <- Array(42, "42", BigInt(42), BigInt, 42.0)) {  
        val result = obj match {
          case x: Int => x
          case s: String => s.toInt
          case _: BigInt => Int.MaxValue
          case BigInt => -1
          case _ => 0
        }
      
        println(result)
    }
    
    for (obj <- Array(Map("Fred" -> 42), Map(42 -> "Fred"), Array(42), Array("Fred"))) {    
        val result = obj match {
          case m: Map[_, _] => "1. It's a Map"             
          case a: Array[Int] => "2. It's an Array[Int]"
          case a: Array[_] => "3. It's an array of something other than Int"
        }
      
        println(result)
    }
}