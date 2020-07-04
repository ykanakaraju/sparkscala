package com.tekcrux.basics

object FunctionsCallByName {
  
   def main(args: Array[String]) {
        delayed( time() );
   }
  
   def time() = {
      println("Getting time in nano seconds")
      System.nanoTime
   }
   
   /*def dummy() = {
      100L
   }*/
   
   def delayed( t: => Long ) = {
      println("In delayed method")
      println("Param: " + t)
   }   
}