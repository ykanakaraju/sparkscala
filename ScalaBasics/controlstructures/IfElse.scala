package com.tekcrux.controlstructures

object ifelse extends App {
   val x = 100;
   val s = if (x > 0) 1 else -1      
   println("value of s = " + s);
   
   def checkVal(x: Int) : String = if (x >= 0) "positive" else "negative"
     
   println("CheckVal >> 10 is " + checkVal(10))
   println("CheckVal >> -10 is " + checkVal(-10))
   
   val a = if (x > 0 && x < 6){ "positive" } else {  0  }
   println(a)   // return type Any    
    
}