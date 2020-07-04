package com.tekcrux.basics

object FunctionsRecursive extends App {
  
   for (i <- 1 to 5)
      println( "Factorial of " + i + ": = " + factorial(i) )
   
   def factorial(n: BigInt): BigInt = {  
      if (n <= 1)
         1  
      else    
      n * factorial(n - 1)
   }
   
   
}