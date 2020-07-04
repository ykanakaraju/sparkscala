package com.tekcrux.higherorderfunctions

import scala.math._

object HigherOrdFnBasics extends App {
  
   // passing a function as a method parameter
    def valueAtOneQuarter(f: (Double) => Double) = f(0.25)   // compare this with func(x: Int) => 2*x
  
    println( valueAtOneQuarter((x:Double) => 3*x) )  // the same statement can be written as follows
    println( valueAtOneQuarter(3 * _) )              // this is parameter inference in action. 
    
    println( valueAtOneQuarter(ceil _) )       // ceil(0.25) = 1.0
    println( valueAtOneQuarter(sqrt _) )       // sqrt(0.25) = 0.5
         
    
    // method returning a function  
    def multiplyBy(factor : Int) = (x : Int) => factor * x
    
    val times4 = multiplyBy(4)
    println (times4(20))   // 4*20 = 80

}