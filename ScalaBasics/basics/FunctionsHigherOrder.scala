package com.tekcrux.basics

object FunctionsHigherOrder extends App {
  
   println( apply( upper, 10) )
   println( apply( upper, "Hello") )

   def apply(f: Any => String, v: Any) = f(v)

   def upper[A](x: A) = "[" + x.toString().toUpperCase() + "]"
}