package com.tekcrux.higherorderfunctions

object scan extends App {
  val fl = List(1, 7, 2, 9).scanLeft(0)(_ - _) 
  val fr = List(1, 7, 2, 9).scanRight(0)(_ - _)
  
  println(fl)
  println(fr)
  
  val fl1 = List(1, 7, 2, 9).scanLeft("A")(_ + _) 
  val fr1 = List(1, 7, 2, 9).scanRight("A")(_ + _)
  
  println(fl1)
  println(fr1)
}