package com.tekcrux.higherorderfunctions

object UsfulHigherOrdFns extends App {
  (1 to 5).map(10 * _).foreach(println _)
   println("---------------------")
  (1 to 5).map("*" * _).foreach(println _)
   
  println("---------------------")
  
  (1 to 9).filter(_ % 2 == 0).foreach(x => print(x + ","))
  
  println
  println("---------------------")

  println ( (1 to 4).reduceLeft(_ - _) )    // (((1-2)-3)-4) => 1-2-3-4 = -8
  println ( (1 to 4).reduceRight(_ - _) )   // (1-(2-(3-4)) => 1-2+3-4 = -2
  
  println("---------------------")
  "Mary once had a little lamb".split(" ").sortWith(_.length < _.length).foreach(println)
  
  println("---------------------")
}