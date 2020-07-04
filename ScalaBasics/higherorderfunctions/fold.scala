package com.tekcrux.higherorderfunctions

object fold extends App {
  
  val fl = List(1, 7, 2, 9).foldLeft(0)(_ - _) 
  val fr = List(1, 7, 2, 9).foldRight(0)(_ - _)
  
  println(fl)
  println(fr)
  
  val fl1 = List(1, 7, 2, 9).foldLeft("A")(_ + _) 
  val fr1 = List(1, 7, 2, 9).foldRight("A")(_ + _)
  
  println(fl1)
  println(fr1)
}