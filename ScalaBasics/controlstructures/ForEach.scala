package com.tekcrux.controlstructures

object ForEach extends App {
     var str1 = "SCALA"
     str1.foreach(println)
     
     println("-------------------------------")
     
     var arr = Array(1,2,3,4,5)
     arr.foreach(a => print(a + " / "))
     
     println("")
     println("-------------------------------")
     
     (1 to 5).foreach(print)
     
}