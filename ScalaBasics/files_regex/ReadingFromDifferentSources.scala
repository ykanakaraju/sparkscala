package com.tekcrux.files_regex

object ReadingFromDifferentSources extends App {
  
  import scala.io.Source

  val source1 = Source.fromURL("http://tekcrux.com", "UTF-8")
  println(source1.mkString.length + " bytes")
    
  val source2 = Source.fromString("Hello, World!")  
  println(source2.mkString.length + " bytes")
    
  println("What is your name?")
  val source3 = Source.stdin  // Reads from standard input    
  println("Hello, " + source3.getLines.next)
  
  println();
  
  val source4 = Source.fromFile("./file1.txt", "UTF-8")   
  source4.getLines().foreach(println)
  
  
}