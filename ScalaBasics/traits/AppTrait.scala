package com.tekcrux.traits

object AppTrait extends App {
  
  // https://www.scala-lang.org/api/current/scala/App.html
  
  println(s"Number of command line args: ${args.length}")
  
  println(args.mkString(","))
  
  println( this.executionStart )
}