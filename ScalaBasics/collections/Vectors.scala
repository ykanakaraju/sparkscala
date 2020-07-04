package com.tekcrux.collections

object Vectors extends App {
  
  val vec = (1 to 1000000) map (_ % 100)  // map transforms a Range into a Vector  
  val lst = vec.toList   // list is List[Int] 
  
  def time[T](block: => T) = {
    val start = System.nanoTime
    val result = block
    val elapsed = System.nanoTime - start
    println(elapsed + " nanoseconds")
    result
  }
  
  time(vec(500000))
  
  time(lst(500000))
}