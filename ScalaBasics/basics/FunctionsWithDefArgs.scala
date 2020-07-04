package com.tekcrux.basics

import scala.math._

object FunctionsWithDefArgs {
  
   def main(args: Array[String]) {
      println( "Returned Value : " + addInt() );         
      println( "Returned Value : " + addInt(2) );        
      println( "Returned Value : " + addInt(2, 4) );     
      println( "Returned Value : " + addInt(b = 100) );  
   }
   
   def addInt( a:Int = 5, b:Int = 7 ) : Int = {
      var sum:Int = 0
      sum = a + b

      return sum
   }
   
}