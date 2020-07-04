package com.tekcrux.higherorderfunctions

object collect extends App {
  
   
    val y = List(7, 15, 9).collect{ case x if (x % 5) != 0 => x * 5 } 
    println(y)  
    
    
    val greaterThan20: PartialFunction[Any, Int] = 
    {
      case i: Int if i > 20 => i
    }
    
    val l = List(1, 45, 10, "blah", true, 25).collect(greaterThan20)
    println(l)
        
}