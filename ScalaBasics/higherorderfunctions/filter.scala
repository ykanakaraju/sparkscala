package com.tekcrux.higherorderfunctions

object filter extends App {
  
   for (i <- 1 to 5) println( factorial(i) )
   
   def factorial(n: BigInt): BigInt = {  
      if (n <= 1) 1  
      else n * factorial(n - 1)
   }

}


