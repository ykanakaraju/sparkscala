package com.tekcrux.basics

object FunctionsNested extends App {
  
   println( factorial(5) )   
   
   def factorial(i: Int): Int = {
      def fact(i: Int, accumulator: Int): Int = {
         if (i <= 1)
            accumulator
         else
            fact(i - 1, i * accumulator)
      }
      fact(i, 1)
   }
}