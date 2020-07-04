package com.tekcrux.basics

object Procedure extends App{
  
    box("Hello Scala")
   
    def box(s : String) {                           // Look carefully: no =
      val border = "-" * s.length + "--\n"
      println(border + "|" + s + "|\n" + border)
    }
}