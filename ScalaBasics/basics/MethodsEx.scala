package com.tekcrux.basics

object MethodsEx extends App {
  
  import scala.math._

  println( sqrt(2) )
  println( pow(2, 4) )
  println( min(3, Pi) )
  
  println( "Hello".distinct )
  
  println( BigInt.probablePrime(100, scala.util.Random) )
}