package com.tekcrux.higherorderfunctions

import scala.math._

object FPBasics extends App {
  
    val fnc = ceil _                    // assigning a function to a variable
    //println(fnc(1.34))                  // 2
    
    val arr1 = Array(15.25, 16.28, 54.0).map(fnc)     // Array(16.0,17.0,54.0)
    printArray(arr1, 1)
    
    val arr2 = Array(3.5, 1.25, 0.67).map((x: Double) => 2 * x)  //
    printArray(arr2, 2)
    
    val arr3 = Array(2,4,5,7,10).filter(_ % 2 == 0)  
    printArray(arr3, 3)
    
    println()  
   
    def m(x: Int) = 2*x       
    println("m(2) = " + m(2))
    //println(m)    // what happens???
     
     
    val f = (x: Int, y: Int) => 2*x*y
    println(f)    // but you can do a println(f) because a method can not be a final value of an expression.
    println(f(2,3)) 
    //print(f.getClass.getSimpleName)
    
    def printArray(arr: Array[_], i: Int) {
       print("\n" + i + " --> ")
       print(arr.mkString("<", ", ", ">"))    // mkSring converts an array into a string       
    } 
}

