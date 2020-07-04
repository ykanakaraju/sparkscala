package com.tekcrux.basics

import scala.io.StdIn

object InputExample extends App {  
  val name = StdIn.readLine("Enter your name: ")
  println("Enter your age: ")
  val age = StdIn.readInt() 
  println(s"Name: ${name}  Age: ${age}") 
 
  
}