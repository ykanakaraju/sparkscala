package com.tekcrux.controlstructures
 
import scala.math.sqrt

object Exceptions extends App {  
  
    def squareRoot(x: Double) = {
       if (x >= 0) { 
          sqrt(x) 
       } 
       else throw new IllegalArgumentException("x should not be negative")
    }
    
    try {
       println(squareRoot(16))
       println(squareRoot(-16))
    } 
    catch {     
       //case _: IllegalArgumentException => println("Caught IllegalArgumentException from the catch block")
       case e: Exception => println(e.getMessage())
    }  
}



