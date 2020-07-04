package com.tekcrux.basics

object ApplyEx extends App {
  println ( "Hello"(3) )

  println ( "Hello".apply(3) )
  
  println ( BigInt("1234567890") )
  
  println ( BigInt.apply("1234567890") )
  
  println ( BigInt("1234567890") * BigInt("112358111321") )
}