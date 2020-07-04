package com.tekcrux.collections

object Streams extends App {
  
  def numsFrom(n: BigInt): Stream[BigInt] = n #:: numsFrom(n + 1)
  val tenOrMore = numsFrom(10)
  println(tenOrMore)
  
  val more = tenOrMore.tail.tail.tail
  println(more)
  
  val squares = numsFrom(1).map(x => x * x)
  println(squares)
  
  val squares5 = squares.take(5).force
  println(squares5)
  
  import scala.io.Source
  
  //val words = Source.fromFile("/usr/share/dict/words").getLines.toStream
  //words 
  //words(5) 
  //words 
}