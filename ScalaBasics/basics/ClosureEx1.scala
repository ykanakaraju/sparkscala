package com.tekcrux.basics

object ClosureEx1 extends App {
   
   /*
   * A function whose return value depends on variable(s) 
   * declared outside it, is a closure
   * 
   */
  
   // Here sum is trivially closed over itself
   val sum = (a:Int, b:Int) => a + b
   println ( sum(10, 20) )  
   
   var c = 5
   
   // Here sumC is a closure that refers to an external variable
   val sumC = (a:Int, b:Int) => (a + b) * c
   println ( sumC(10, 20) ) 
   
   c = 4
   println ( sumC(10, 20) ) 
   
   // Another example..
   var age = 22
   val helloFn = (name: String) => println(s"My name is $name and I am $age years old")
   
   helloFn("Raju")
   
   age = 25
   def func(f:String => Unit, s:String) =  f(s)
   func(helloFn, "Ken")
}